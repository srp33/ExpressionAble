FROM python:3.6-stretch
RUN pip3 uninstall -y scipy numpy scikit-learn
RUN pip3 install numpy==1.15.4
RUN pip3 install pandas
RUN pip3 install --no-cache-dir pyarrow sqlalchemy xlsxwriter tables xlrd nbformat
ADD RunTests.sh /
ADD RunTests2.sh /
ADD Tests/InputData /Tests/InputData
ADD Tests/OutputData /Tests/OutputData
ADD *.py /
ADD VERSION /ShapeShifter/
RUN mv setup.py /ShapeShifter/
ADD shapeshifter /ShapeShifter/shapeshifter
RUN pip3 install -e ShapeShifter
