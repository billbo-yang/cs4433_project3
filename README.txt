INSTALLATION:
Python3 and pip are prerequisites for this project.
To install all dependencies, run 'pip install -r requirements.txt'

RUN INSTRUCTIONS:
In order to generate the needed datasets for problems 1 & 2, oe must
run create_datasets.py and create_datasets_p2.py. Please note that
by default these two scripts product data files that are very large
and may take some time to fully execute. If you wish to create smaller
datasets you must go into the scripts and change the magic numbers.
Line 25 for create_datasets.py and lines 66 and 70 for
create_datasets_p2.py.

You can run the data generation scripts like so:
'python3 create_datasets.py'
'python3 create_datasets_p2.py'

After the datasets are generated, you can run the scripts for problem 1
by running p1_query1.py and p1_query2.py like so:
'python3 p1_query1.py'
'python3 p1_query2.py'

The above scripts will output into folders called 'temp/' and 'temp2/'.

For problem 2, you can run p2.py like so:
'python3 p2.py'

The above script will write the required output into the terminal.

IMPLEMENTATION DETAILS AND RELATIVE CONTRIBUTIONS
Please see the report document for implementation details and relative
contributions