# Big Data Analysis with Scala and Spark
### Week 1

-----------------------------
Exercise: Wikipedia
-----------------------------
In this assignment, you will get to know Spark by exploring full-text Wikipedia articles.

Gauging how popular a programming language is important for companies judging whether or not they should adopt an emerging programming language. For that reason, industry analyst firm RedMonk has bi-annually computed a ranking of programming language popularity using a variety of data sources, typically from websites like GitHub and StackOverflow. See their top-20 ranking for June 2016 as an example.

In this assignment, we'll use our full-text data from Wikipedia to produce a rudimentary metric of how popular a programming language is, in an effort to see if our Wikipedia-based rankings bear any relation to the popular Red Monk rankings.

You'll complete this exercise on just one node (your laptop), but you can also head over to Databricks Community Edition to experiment with your code on a "micro-cluster" for free.

### Set up Spark

For the sake of simplified logistics, we'll be running Spark in "local" mode. This means that your full Spark application will be run on one node, locally, on your laptop.

To start, we need a 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚝𝚎𝚡𝚝. A 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚝𝚎𝚡𝚝 is the "handle" to your cluster. Once you have a 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚝𝚎𝚡𝚝, you can use it to create and populate RDDs with data.

To create a 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚝𝚎𝚡𝚝, you need to first create a 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚏𝚒𝚐 instance. A 𝚂𝚙𝚊𝚛𝚔𝙲𝚘𝚗𝚏𝚒𝚐 represents the configuration of your Spark application. It's here that you must specify that you intend to run your application in "local" mode. You must also name your Spark application at this point. For help, see the Spark API Docs.

Configure your cluster to run in local mode by implementing 𝚟𝚊𝚕 𝚌𝚘𝚗𝚏 and 𝚟𝚊𝚕 𝚜𝚌.

### Read-in Wikipedia Data

There are several ways to read data into Spark. The simplest way to read in data is to convert an existing collection in memory to an RDD using the 𝚙𝚊𝚛𝚊𝚕𝚕𝚎𝚕𝚒𝚣𝚎 method of the Spark context.

We have already implemented a method 𝚙𝚊𝚛𝚜𝚎 in the object 𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙳𝚊𝚝𝚊 object that parses a line of the dataset and turns it into a 𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙰𝚛𝚝𝚒𝚌𝚕𝚎.

Create an 𝚁𝙳𝙳 (by implementing 𝚟𝚊𝚕 𝚠𝚒𝚔𝚒𝚁𝚍𝚍) which contains the 𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙰𝚛𝚝𝚒𝚌𝚕𝚎 objects of 𝚊𝚛𝚝𝚒𝚌𝚕𝚎𝚜.

### Compute a ranking of programming languages

We will use a simple metric for determining the popularity of a programming language: the number of Wikipedia articles that mention the language at least once.

Rank languages attempt #1: rankLangs

##### Computing 𝚘𝚌𝚌𝚞𝚛𝚛𝚎𝚗𝚌𝚎𝚜𝙾𝚏𝙻𝚊𝚗𝚐
Start by implementing a helper method 𝚘𝚌𝚌𝚞𝚛𝚛𝚎𝚗𝚌𝚎𝚜𝙾𝚏𝙻𝚊𝚗𝚐 which computes the number of articles in an 𝚁𝙳𝙳 of type 𝚁𝙳𝙳[𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙰𝚛𝚝𝚒𝚌𝚕𝚎𝚜] that mention the given language at least once. For the sake of simplicity we check that it least one word (delimited by spaces) of the article text is equal to the given language.

##### Computing the ranking, 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜
Using 𝚘𝚌𝚌𝚞𝚛𝚛𝚎𝚗𝚌𝚎𝚜𝙾𝚏𝙻𝚊𝚗𝚐, implement a method 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜 which computes a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

An example of what 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜 might return might look like this, for example:
```
List(("Scala",999999),("JavaScript",1278),("LOLCODE",982),("Java",42))
```
The list should be sorted in descending order. That is, according to thisranking, the pair with the highest second component (the count) should be thefirst element of the list.

Hint: You might want to use methods 𝚏𝚕𝚊𝚝𝙼𝚊𝚙 and 𝚐𝚛𝚘𝚞𝚙𝙱𝚢𝙺𝚎𝚢 on 𝚁𝙳𝙳 for this part.

Pay attention to roughly how long it takes to run this part! (It should take tens of seconds.)

Rank languages attempt #2: rankLangsUsingIndex

##### Compute an inverted index

An inverted index is an index data structure storing a mapping from content, such as words or numbers, to a set of documents. In particular, the purpose of an inverted index is to allow fast full text searches. In our use-case, an inverted index would be useful for mapping from the names of programming languages to the collection of Wikipedia articles that mention the name at least once.

To make working with the dataset more efficient and more convenient, implement a method that computes an "inverted index" which maps programming language names to the Wikipedia articles on which they occur at least once.

Implement method 𝚖𝚊𝚔𝚎𝙸𝚗𝚍𝚎𝚡 which returns an RDD of the following type: 𝚁𝙳𝙳[(𝚂𝚝𝚛𝚒𝚗𝚐, 𝙸𝚝𝚎𝚛𝚊𝚋𝚕𝚎[𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙰𝚛𝚝𝚒𝚌𝚕𝚎])]. This RDD contains pairs, such that for each language in the given 𝚕𝚊𝚗𝚐𝚜 list there is at most one pair. Furthermore, the second component of each pair (the 𝙸𝚝𝚎𝚛𝚊𝚋𝚕𝚎) contains the 𝚆𝚒𝚔𝚒𝚙𝚎𝚍𝚒𝚊𝙰𝚛𝚝𝚒𝚌𝚕𝚎𝚜 that mention the language at least once.

##### Computing the ranking, 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜𝚄𝚜𝚒𝚗𝚐𝙸𝚗𝚍𝚎𝚡
Use the 𝚖𝚊𝚔𝚎𝙸𝚗𝚍𝚎𝚡 method implemented in the previous part to implement a faster method for computing the language ranking.

Like in part 1, 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜𝚄𝚜𝚒𝚗𝚐𝙸𝚗𝚍𝚎𝚡 should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

Again, the list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.

Hint: method 𝚖𝚊𝚙𝚅𝚊𝚕𝚞𝚎𝚜 on 𝙿𝚊𝚒𝚛𝚁𝙳𝙳 could be useful for this part.

Can you notice a performance improvement over attempt #2? Why?

Rank languages attempt #3: rankLangsReduceByKey

In the case where the inverted index from above is only used for computing the ranking and for no other task (full-text search, say), it is more efficient to use the 𝚛𝚎𝚍𝚞𝚌𝚎𝙱𝚢𝙺𝚎𝚢 method to compute the ranking directly, without first computing an inverted index. Note that the 𝚛𝚎𝚍𝚞𝚌𝚎𝙱𝚢𝙺𝚎𝚢 method is only defined for RDDs containing pairs (each pair is interpreted as a key-value pair).

Implement the 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜𝚁𝚎𝚍𝚞𝚌𝚎𝙱𝚢𝙺𝚎𝚢 method, this time computing the ranking without the inverted index, using 𝚛𝚎𝚍𝚞𝚌𝚎𝙱𝚢𝙺𝚎𝚢.

Like in part 1 and 2, 𝚛𝚊𝚗𝚔𝙻𝚊𝚗𝚐𝚜𝚁𝚎𝚍𝚞𝚌𝚎𝙱𝚢𝙺𝚎𝚢 should compute a list of pairs where the second component of the pair is the number of articles that mention the language (the first component of the pair is the name of the language).

Again, the list should be sorted in descending order. That is, according to this ranking, the pair with the highest second component (the count) should be the first element of the list.

Can you notice an improvement in performance compared to measuring both the computation of the index and the computation of the ranking as we did in attempt #2? If so, can you think of a reason?
