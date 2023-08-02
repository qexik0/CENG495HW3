# README
After inspecting the data, it was found that there are missing values and there are commas in some columns, for that reason I firstly converted csv file to tsv file using python script that I supplied:

    python3 to_tsv.py movies.csv
	
 It outputs a movies.tsv file, but it uses a very popular library called pandas for it, it is probably already installed, but if not, it may have to be installed:

	pip3 install pandas
	python3 to_tsv.py movies.csv

Alternatively, I supply the ready tsv file.
Other than replacing the movies.csv path to movies.tsv files when evaluating the program, no changes are needed.
