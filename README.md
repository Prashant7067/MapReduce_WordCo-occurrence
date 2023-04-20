# MapReduce_WordCo-occurrence
How to run .jar file

Create files with the command nano <file name> as demo file.txt

Enter few sentences in this file and save it.

Use the command hdfs dfs -mkdir /<folder name> as hdfs dfs -mkdir /ii to create a folder in HDFS.

Use the command hdfs dfs -put <file name> /<folder name> as hdfs dfs -put file.txt /ii to place all files in the folder.

Using Eclipse IDE - Create a Java Project

Create a new Class and use the above code

Save the file and right click and export as jar file

Run the.jar file by typing hadoop jar <jar file> <class> /<folder name> /<folder name>/output as hadoop jar index.jar InvertedIndex /ii /ii/output
