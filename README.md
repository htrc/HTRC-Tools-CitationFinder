# HTRC-Tools-CitationFinder
This app processes a corpus of text to find all volumes containing potential citations
of other volumes in the corpus.  The candidate citations are found by looking at
each volume in the corpus in chronological order and searching for mentions of author
last names and volume (short) title in each of the volumes published "later" (chronologically)
than the volume in the current loop iteration.

# Build
* To generate an executable package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.

# Run
The following command line arguments are available:
```
find-citations
HathiTrust Research Center
  -l, --log-level  <LEVEL>    The application log level; one of INFO, DEBUG, OFF
  -n, --num-partitions  <N>   The number of partitions to split the input set of
                              HT IDs into, for increased parallelism
  -o, --output  <DIR>         Write the output to the file `citations.csv` in
                              DIR
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --spark-log  <FILE>     Where to write logging output from Spark to
      --help                  Show help message
      --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```

*Note*: The app requires corpus to exist in Pairtree format and additionally include the volume 
JSON metadata at the leaf level (in the same folder as the .zip and .mets.xml file)
