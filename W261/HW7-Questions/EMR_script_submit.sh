aws emr create-cluster --name "Test cluster" --ami-version 3.10 --applications Name=Hue Name=Hive Name=Pig \
--use-default-roles --ec2-attributes KeyName=myKey \
--instance-type m3.xlarge --instance-count 3 \
--steps Type=CUSTOM_JAR,Name=CustomJAR, \
ActionOnFailure=CONTINUE, \
Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar, \
Args=["s3://mybucket/script-path/my_script.sh"]
