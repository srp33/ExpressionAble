FROM python:3.7-stretch
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
