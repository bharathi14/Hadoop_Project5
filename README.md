# Hadoop_Project5
MapReduce Project-Webdata
Description
Programming Assignment 5 Using Map-Reduce
Due on Sunday July 31 before midnight
The purpose of this project is to develop a simple Map-Reduce program on Hadoop to analyze data.
This project must be done individually. No copying is permitted. Note: We will use a system for detecting software plagiarism. That is, your program will be compared with the programs of the other students in class as well as with the programs submitted in previous years. This program will find similarities even if you rename variables, move code, change code structure, etc.
Note that, if you use a Search Engine to find similar programs on the web, we will find these programs too. So don't do it because you will get caught and you will get an F in the course (this is cheating). Don't look for code to use for your project on the web or from other students (current or past). Just do your project alone using the help given in this project description and from your instructor and GTA only.
Platform
You will develop this project on Omega. Look at the directions at omega.uta.edu student web page. After you login on Omega, do the following:
export HADOOP=/home/u/ux/uxg4406/public_html/project9/hadoop-1.2.1 export CLASSPATH=.:$HADOOP/hadoop-core-1.2.1.jar
export JAVA_HOME=/opt/jdk1.6.0_20
Hadoop can be run from your Hadoop directory with the following command in STANDALONE MODE. $HADOOP/bin/hadoop jar YourJarFile.jar YourMainClass YourProgramArguments1 YourProgramArguments2
For Python, following command:
$HADOOP/bin/hadoop jar $HADOOP/contrib/streaming/hadoop-streaming-1.2.1.jar -input yourinputfolder -output youroutputfolder -mapper YourMapper.py -reducer YourReducer.py
Documentation
•A Map-Reduce example that does a join.
•The The Map-Reduce API. The API has two variations for most
classes:org.apache.hadoop.mapreduce and org.apache.hadoop.mapred. You should only use the classes in the package org.apache.hadoop.mapreduce
•The org.apache.hadoop.mapreduce package
•The Job class
•Download example.zip from UTA Blackboard. It contains example of word count both in Java and Python
￼￼￼￼￼
Project Requirements
Download the project5.zip from the UTA Blackboard. It contains the input documents. You need to implement the TF-IDF in the Map-Reduce framework. For the detailed description of TF-IDF, refer to the lecture slides 8. Also refer to WordCount example in lecture slides 9 for the MapReduce program. For the list of stopwords, Click here.
For the computation of the tf-idf of documents. You require 4 MapReduce Jobs: 1. Count the occurrence of each word in each document.
• Map:
◦ Input(key=LineNr,value=Lineindocument)
◦ Tokenizethelineintowordsandoutputeachword.
◦ Checkwhetherthewordisthestopwordsornot.
◦ Ifthewordisnotthestopwordthen,Emit(key=word&filename,value=1)
• Reduce:
◦ Input(key=word&filename,values=[counts]) ◦ Sumallthecountsasn
◦ Emit(key=word&filename,value=n)
2. Count the total number of words per document.
• Map:
◦ Input(key=word&filename,value=n)
◦ Theoutputkeyforthemapperwillbeonlyfilename,andmovethewordfromtheoldkeyinto
output value
◦ Emit(key=filename,value=word&n)
• Reduce:
◦ Input (key = filename, values = [word&n])
◦ Sum all the n's in the whole document as N and output every word again. You may have to iterate thrugh the list twice. One to sum all the n's and one to write out all words again one at a time.
◦ Iterators are one-traversal-only - You will have to store the values somewhere such as ArrayList or HashMap in Java and List in Python.
◦ Emit (key = word&filename, value = n&N)
3. Count total number of documents in which each word occurred.
￼
• Map:
◦ Input(key=word&filename,value=n&N)
◦ Appendthefilenamefrominputkeytooutputvaluefieldandadd1totheendofoutputvalue. ◦ Output(key=word,value=filename&n&N&1)
• Reduce:
◦ Input(key=word,values=[filename&n&N&1])
◦ Calculatethesumofvaluefielslastentriesasmandmovefilenamebacktokey
◦ Again,youwillhavetolookthroughvaluestwice,oncetofindthesumandoncetoprintoutallthe entries again. And you will have to store the values somewhere again.
◦ Output(key=word&filename,value=n&N&m) 4. Compute the TF-IDF of each of the document.
• Map:
◦ Input(key=word&filename,value=n&N&m)
◦ calculateTD-IDFbasedonn&&mandD(numberofdocuments).Disknownaheadoftime.
◦ TFIDF=n/N*log(D/m)
◦ NOTE: Don't divide integers with integers. You should use doubles instead, or division will not work properly.
◦ Output(key=word&filename,value=TF-IDF)
• There is no Reduce in this job. Map output is directly written out.
HINT:
• For getting filename in mapper in Java:
◦ String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
• For getting filename in mapper in Python:
◦ filename = os.environ["map_input_file"]
• You need to specify number of reducers 0 for Map Only Job.
◦ Python: ­numReduceTasks 0
◦ Java: conf.setNumReduceTasks(0)
