# Running the script
Ensure that ds-server, ds-client, and your client are in this directory.
Run `ds_test.py`, passing the command to run your client as a string and -n if your client uses newline-terminated messages (i.e. Python or Java clients).
You can specify a directory of config files with -c (`TestConfigs` is used by default if you omit -c).
You will also need to specify your port number with `-p` (use a unique port number, like 5 followed by the last 4 digits of your ID).
For example:
```
python3 ./ds_test.py "python3 client.py" -n -p 50000 -c TestConfigs
```

To view the usage message that explains all options, use `-h`.
For example:
```
python3 ./ds_test.py -h
```

To speed up testing, results from the reference client using the configs in TestConfigs are read from `results/ref_results.json`.
However, if you want to try different configs you can make the script run them with ds-client and store the results in a new file.
The script will run ds-client when `--process_reference_client` is provided, and you can specify a new reference results file with `-r`.
For example (with a new set of configs stored in the NewConfigs directory):
```
python3 ./ds_test.py "python3 client.py" -n -p 50000 -c NewConfigs --process_reference_client -r results/new_ref_results.json
```

If you made a mistake when running the script (e.g. misspelled your client name or forgot `-n`), then kill the script with CTRL+C and try again.

# Output explanation
When the script is finished, it will print out three tables (one for each metric).
The leftmost column lists the configuration files.
The middle columns list the results of each baseline algorithm for each respective config.
The rightmost column lists your client's results for each respective config.
After the config rows, there is a row of averages and several rows with normalised values.
These final values are the average result of each column normalised against the average value of that row's algorithm.
The last "average" row depicts the average result of each column against the average results of all baseline algorithms.

The colour of values in the "Yours" column indicates your client's performance in comparison to the baseline algorithms.
Green indicates your client outperformed all baseline algorithms.
Yellow indicates your client outperformed at least one baseline algorithms.
Red indicates your client performed worse than all baseline algorithms.

The final results are shown after the tables.
"Handshake" indicates whether your client has successfully completed the handshake with ds-server (and gracefully terminated with a `QUIT` message).
"Average Performance" indicates that your client has outperformed the average of at least one baseline algorithm for all metrics.
"Turnaround Performance" indicates the number of configs where your client outperforms all baseline algorithms for average turnaround time (maximum of 7).
