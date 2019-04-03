FROM python:3.7-stretch
ADD RunTests.sh /
ADD RunTests2.sh /
ADD Tests/InputData /Tests/InputData
ADD Tests/OutputData /Tests/OutputData
ADD *.py /
ADD VERSION /ShapeShifter/
ADD MANIFEST.in /ShapeShifter/
ADD README.md /ShapeShifter/
RUN mv setup.py /ShapeShifter/
ADD shapeshifter /ShapeShifter/shapeshifter
RUN pip3 install -e ShapeShifter
