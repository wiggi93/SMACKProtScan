scp -r -i /Users/philipwiegratz/Desktop/WorkspaceSpeL/dcoskey_bigi.pem /Users/philipwiegratz/Desktop/WorkspaceSpeL/SMACKProtScan/target/SparkTest-jar-with-dependencies.jar centos@129.70.51.18:/home/centos/test/test.jar;

ssh -i /Users/philipwiegratz/Desktop/WorkspaceSpeL/SMACKProtScan/dcoskey_bigi.pem centos@129.70.51.18 'bash ./upload.sh $3';


scp -r -i genconf/ssh_key ./test/test.jar ubuntu@192.168.0.12:/home/ubuntu/test/test.jar


ds1=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "192.168.0.21:9092").option("subscribe", "testtopic").option("startingOffsets" , "earliest").load()

ds1.writeStream.format("console").start