### 1. Get Linux
FROM python:3.7-stretch

# Install OpenJDK-8
RUN apt-get update && \
apt-get install -y openjdk-8-jdk && \
apt-get install -y ant && \
apt-get clean;

# Fix certificate issues
RUN apt-get update && \
apt-get install ca-certificates-java && \
apt-get clean && \
update-ca-certificates -f;
# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

ADD RunTests.sh /
ADD RunTests2.sh /
ADD Tests/InputData /Tests/InputData
ADD Tests/OutputData /Tests/OutputData
ADD *.py /
ADD VERSION /ExpressionAble/
ADD MANIFEST.in /ExpressionAble/
ADD README.md /ExpressionAble/
RUN mv setup.py /ExpressionAble/
ADD expressionable /ExpressionAble/expressionable
RUN pip3 install -e ExpressionAble
