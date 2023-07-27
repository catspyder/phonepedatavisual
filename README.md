# phonepedatavisual

the task at hand is to unpack and go throught over 4000 files spread across 100s of folders and visualize them in a dashboard.
there are 3 main folders each having to subfolders of transaction and users. each of these need to be handled differently

first off all we extracted all file paths using loops and use these paths to extract the files. 
there are 6 functions to extract the data from the 6 types of files and convert them to dataframes.
these dataframes are then stored in a dictionary called datadict for easy access.
the extracted datasets are then stored to sql using pandas to_sql function
a seperate app.py function is used to convert the stored data into a useful graphs.
the files from the map folder are shown as maps
while the files from the other folders are shown as bar graphs
