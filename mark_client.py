#!/usr/bin/env python3
import glob
import json
import operator
import os
import re
import subprocess
import sys
from pathlib import Path
from statistics import mean
from time import sleep
from typing import Union, Dict, List

BOLD = "\033[1m"
RED = "\033[31m"
YELLOW = "\033[33m"
GREEN = "\033[32m"
END = "\033[0m"

ClientResultDict = Dict[str, Dict[str, Union[int, float, None]]]
RefResultDict = Dict[str, Dict[str, Dict[str, Union[int, float]]]]

re_time = re.compile(r".*avg turnaround time: (\d+)")
re_util = re.compile(r".*avg util: (\d+\.?\d*).*")
re_cost = re.compile(r".*total cost: \$(\d+\.?\d*)")
re_jobs = re.compile(r".*#jobs: (\d+).*")
re_unscheduled_jobs = re.compile(r"(\d+) jobs not scheduled!")

baseline = ["atl", "ff", "bf", "fc", "fafc"]
base_num = len(baseline)

config_width = 28
metric_width = 10
bold_width = metric_width + len(BOLD) + len(END)
colour_width = metric_width + len(RED) + len(END)
base_row_template = "|".join(["{{:<{}}}"] * (base_num + 2))
plain_row_template = base_row_template.format(*[config_width] + [metric_width] * (base_num + 1))
normal_row_template = base_row_template.format(*[config_width] + [metric_width] * base_num + [colour_width])
average_row_template = base_row_template.format(
    *[config_width + len(BOLD) + len(END)] + [bold_width] * base_num + [bold_width + len(RED)])


def colour_text(metric_: Union[int, float], score_: int, template: str) -> str:
    if score_ == base_num:
        return template.format(GREEN, metric_, END)
    elif score_ > 0:
        return template.format(YELLOW, metric_, END)
    else:
        return template.format(RED, metric_, END)


def check_required(config_dir: str):
    conf_dir = Path(config_dir)
    if not conf_dir.exists():
        print("Error: config directory '{}' does not exist".format(conf_dir), file=sys.stderr)
        sys.exit(1)

    ds_server = Path("./ds-server")
    if not ds_server.exists():
        print("Error: ds-server does not exist", file=sys.stderr)
        sys.exit(1)


def is_extra_config(config_filename: str) -> bool:
    return config_filename.endswith(".ext.xml")


def is_number(value) -> bool:
    return isinstance(value, (int, float))


def parse_client_results(
        conf_dir: str,
        metrics: List[str],
        command: str,
        newline: bool,
        port: int
) -> ClientResultDict:
    results: ClientResultDict = {metric: {} for metric in metrics + ["Scheduled jobs", "Unscheduled jobs"]}

    for config in sorted(glob.glob(os.path.join(conf_dir, "*.xml"))):
        config_name = os.path.basename(config)
        print("Running client with", config_name)

        results["Turnaround time"][config_name] = None
        results["Resource utilisation"][config_name] = None
        results["Total rental cost"][config_name] = None
        results["Scheduled jobs"][config_name] = None
        results["Unscheduled jobs"][config_name] = None

        server_command = ["./ds-server", "-c", config, "-v", "brief", "-p", str(port)]
        if newline:
            server_command.append("-n")

        server_p = subprocess.Popen(server_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sleep(4)
        client_p = subprocess.Popen(command.split())
        server_out, server_err = server_p.communicate()

        server_p.wait()
        client_p.wait()

        if server_err:
            server_err_message = server_err.decode("utf-8")
            print("Error encountered by ds-server:\n", server_err_message, file=sys.stderr)

            unscheduled_jobs_match = re_unscheduled_jobs.match(server_err_message)
            if unscheduled_jobs_match:
                results["Unscheduled jobs"][config_name] = int(unscheduled_jobs_match.group(1))

        server_lines = server_out.splitlines()

        if len(server_lines) < 3:
            print("Error: could not parse server output", file=sys.stderr)
            continue

        lines = list(map(str, server_lines[-3:]))
        time_match = re_time.match(lines[2])
        util_match = re_util.match(lines[1])
        cost_match = re_cost.match(lines[1])
        jobs_match = re_jobs.match(lines[0])

        if not time_match or not util_match or not cost_match or not jobs_match:
            print("Error: could not parse server output", file=sys.stderr)
            continue

        time = time_match.group(1)
        util = util_match.group(1)
        cost = cost_match.group(1)
        jobs = jobs_match.group(1)

        results["Turnaround time"][config_name] = int(time)
        results["Resource utilisation"][config_name] = float(util)
        results["Total rental cost"][config_name] = float(cost)
        results["Scheduled jobs"][config_name] = int(jobs)

    res_path = Path("results/test_results.json")
    res_path.parent.mkdir(parents=True, exist_ok=True)

    with open(res_path, 'w') as client_results_file:
        json.dump(results, client_results_file, indent=2)

    print()
    return results


def print_results(
        client_results: ClientResultDict,
        ref_results: RefResultDict,
        metrics: List[str],
        objective: str
):
    mark_handshake = 1
    baseline_scores = {}
    average_scores = {}

    for metric in metrics:
        if not client_results[metric] or all(value is None for value in client_results[metric].values()):
            print("Error: no results for {}".format(metric), file=sys.stderr)
            mark_handshake = 0
            continue

        print(metric)
        print(plain_row_template.format(*["Config"] + [algo.upper() for algo in baseline] + ["Yours"]))

        baseline_scores[metric] = {}
        for config, res in client_results[metric].items():
            comp = operator.gt if metric == "Resource utilisation" else operator.lt

            if not is_number(res):
                print("No results found for", config)
                mark_handshake = 0
                continue

            if client_results["Unscheduled jobs"][config] and client_results["Unscheduled jobs"][config] > 0:
                print(f"Unscheduled jobs for {config}: {client_results['Unscheduled jobs'][config]}")
                continue

            baseline_score = 0
            for algo in baseline:
                if comp(res, ref_results[metric][config][algo]):
                    baseline_score += 1

            baseline_scores[metric][config] = baseline_score

            precision = "{:.2f}" if metric != "Turnaround time" else "{}"
            normal_row_vals = (
                    [config] +
                    [precision.format(ref_results[metric][config][algo]) for algo in baseline] +
                    [colour_text(res, baseline_score, "{{}}{}{{}}".format(precision))]
            )
            print(normal_row_template.format(*normal_row_vals))

        averages = {
            **{algo: mean(res_dict[algo] for config, res_dict in ref_results[metric].items()) for algo in baseline},
            "student": mean(res for res in client_results[metric].values() if is_number(res))
        }

        comp = operator.ge if metric == "Resource utilisation" else operator.le
        average_score = 0
        for algo in baseline:
            if comp(averages["student"], averages[algo]):
                average_score += 1

        average_scores[metric] = average_score

        student_average_string = BOLD + colour_text(averages["student"], average_score, "{}{:.2f}{}")
        averages_string = (
                ["{}{}{}".format(BOLD, "Average", END)] +
                ["{}{:.2f}{}".format(BOLD, averages[algo], END) for algo in baseline] +
                [student_average_string]
        )
        print(average_row_template.format(*averages_string))
        baseline_average = mean((averages[algo] for algo in baseline))

        algos = baseline + ["student"]
        normalised_results = {base: {algo: averages[algo] / averages[base] for algo in algos} for base in baseline}
        normalised_baseline = {algo: averages[algo] / baseline_average for algo in algos}

        for base in baseline:
            norm_string = (
                    ["Normalised ({})".format(base.upper())] +
                    ["{:.4f}".format(normalised_results[base][algo]) for algo in algos]
            )
            print(plain_row_template.format(*norm_string))

        norm_baseline_string = (
                ["Normalised (Average)"] +
                ["{:.4f}".format(normalised_baseline[algo]) for algo in algos]
        )
        print(plain_row_template.format(*norm_baseline_string))
        print()

    mark_scheduling = 0
    average_performance = 0
    max_scheduling = 7
    objectives = ["tt", "ru", "co"]
    objective_dict = {objective: metric for objective, metric in zip(objectives, metrics)}

    if average_scores and all(score > 0 for score in average_scores.values()):
        average_performance = 1

    if (
            objective_dict[objective] in baseline_scores
            and any(scores for scores in baseline_scores.values())
            and average_performance > 0
    ):
        mark_scheduling = min(
            max_scheduling,
            sum(
                1 for config, score in baseline_scores[objective_dict[objective]].items() if score == base_num
            )
        )

    print("Final results:")
    print("Handshake: {}/1".format(mark_handshake))
    print("Average Performance: {}/1".format(average_performance))
    print("Turnaround Performance: {}/{}".format(mark_scheduling, max_scheduling))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--ref", required=True, help="path to ref_results.json")
    parser.add_argument("-c", "--config_dir", required=True, help="path to config directory")
    parser.add_argument("-s", "--server", required=True, help="path to ds-server binary")
    parser.add_argument("-p", "--program", required=True, help="path to your client.py")
    parser.add_argument("-n", "--newline", action="store_true", help="pass -n to ds-server")
    parser.add_argument("--port", type=int, default=50000, help="port number to use")
    args = parser.parse_args()

    check_required(args.config_dir)

    test_metrics = ["Turnaround time", "Resource utilisation", "Total rental cost"]
    ref_results = json.load(open(args.ref))

    client_results = parse_client_results(
        args.config_dir,
        test_metrics,
        command=f"python3 {args.program}",
        newline=args.newline,
        port=args.port
    )

    print_results(
        client_results,
        ref_results,
        test_metrics,
        objective="tt"
    )
