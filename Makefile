CP=`/home/biar/hadoop/bin/hadoop classpath`
BIN = bin/
SRC = src/

all:
	javac -classpath $(CP) -d $(BIN) $(SRC)*.java -Xlint:unchecked 
	jar -cvf WordCount.jar -C $(BIN) .

copy:
	hadoop fs -rm -f -skipTrash /word_count_input.txt
	hadoop fs -put wrc.txt /word_count_input.txt

run:
	hadoop fs -rm -r /out*
	time yarn jar WordCount.jar WordCount -r 2 /word_count_input.txt /out
		
read:
	hadoop fs -cat /out/part-r-00000

start:
	start-dfs.sh
	start-yarn.sh
	
get:
	hadoop fs -get /out/part-r-00000 resultWordCount.txt
	
	

.phony: clean

clean:
	rm -rf $(BIN)* *.jar
