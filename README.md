BoggleMapReduce
===============

A program to use Hadoop MapReduce and Graph Theory to efficiently and scalably find all words in a Boggle roll.

If you haven't ever played Boggle, refer to the (Wikipedia)[http://en.wikipedia.org/wiki/Boggle] page for how to play. To form a word in Boggle, you start with an arbitrary dice's letter and try to form a word with adjoining dices' letters.  Keeping on going through all dice until you have found every word in roll.

How to Use it
=============

To run the program, you'll need to use the following command line:  
hadoop jar BoggleDriver bloom.out /usr/share/dict/linux.words input output  

The bloom.out file is the location of the Bloom Filter serialized to a file.  The /usr/share/dict/linux.words is the location of the dictionary you want to use.  The input directory is where the graph traversal output gets saved.  The output directory is where the final output with the words in the Boggle roll.

You'll need to run the UserDictBloom to create a new bloom file if you want to use a different dictionary than linux.words.

How it works
============

First, we create a Boggle roll.  The letters on the dice are chosen.

Next, a Map only job is run in a loop until the entire graph is traversed or all possible words are found.  The starting letters are passed in to the Mapper as an adjacency list.  The Mapper iterates around all adjoining characters.  Before emitting the adjoining character, the new word is passed through a Bloom Filter for a membership test.  If the new word passes the membership test, it is emitted.

Once the loop is done, the possible words are passed in to a final MapReduce job to verify that the words really appear in the dictionary.  If a word is in the dictionary, the word is emitted.

Benefits of the Bloom Filter
============================

The (Bloom Filter)[http://en.wikipedia.org/wiki/Bloom_filter] allows the program to efficiently and quickly filter out words that don't appear in the dictionary.  Filtering out a word or node early allows you skip the dead ends much quicker.  Throwing out a word early in the iteration pays good dividends because of exponential growth of child nodes.

For example, take this line of log output from the program:  
12/12/29 19:29:48 INFO Boggle: Traversed graph for 11 iterations.  Found 3404 potential words.  Bloom saved 11830 so far.  

Although the Bloom says it saved 11,380 words from being traversed, the real number is much higher because it saved all subsequent generations from being traversed too.
