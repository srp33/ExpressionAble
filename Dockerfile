FROM python:3.7-stretch

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN export JAVA_HOME && \
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean UU && \
    apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f && \
    pip3 install six python-dateutil numpy pytz pandas pyarrow \
                 kiwisolver cycler pyparsing matplotlib sqlalchemy \
                 xlsxwriter mock numexpr tables xlrd decorator \
                 ipython-genutils traitlets jupyter-core attrs \
                 pyrsistent jsonschema nbformat chardet certifi urllib3 \
                 idna requests h5py cmapPy distro tabula-py

COPY Tests/InputData /Tests/InputData
COPY Tests/OutputData /Tests/OutputData
COPY VERSION /ExpressionAble/
COPY MANIFEST.in /ExpressionAble/
COPY README.md /ExpressionAble/
COPY setup.py /ExpressionAble/
COPY RunTests*.sh /
COPY *.py /
COPY expressionable /ExpressionAble/expressionable

RUN pip3 install -e ExpressionAble
