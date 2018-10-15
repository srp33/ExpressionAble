FROM python:3.6-stretch
RUN pip3 uninstall -y scipy numpy scikit-learn
RUN pip3 install --no-binary numpy pandas 
RUN pip3 install --no-cache-dir pyarrow sqlalchemy xlsxwriter tables xlrd nbformat
ADD RunTests.sh /
ADD RunTests2.sh /
ADD Tests/InputData /Tests/InputData
ADD Tests/OutputData /Tests/OutputData
ADD Tests/*.py /
ADD shapeshifter /
