FROM python:3.9
MAINTAINER Data Engineering Analytics Platform (DEAP)

#Create workdirectory & add application files
WORKDIR /src
ADD __init__.py /src

ADD requirements.txt /src
ADD config.ini /src

ADD main.py /src
ADD add.py /src
ADD aws.py /src
ADD cfg.py /src
ADD datanado.py /src
ADD emailer.py /src
ADD exception.py /src
ADD exposure_report.py /src
ADD headers.py /src
ADD jira_util.py /src
ADD queries.py /src
ADD report.py /src
ADD s3.py /src
ADD zfs.py /src

#RUN mkdir -p /src/
RUN mkdir -p /src/tmp

# Install App or Python Libraries
RUN pip install --upgrade pip
RUN pip install -r /src/requirements.txt

# Add user to run application
RUN groupadd -r -g 1001 appuser
RUN useradd -ms /bin/bash -r -u 1001 -g appuser appuser

# Grant root directory access to appuser
RUN chown -R appuser:appuser /src
USER 1001

#ENV to store logs in stdout
ENV PYTHONUNBUFFERED=0

ENTRYPOINT ["python", "/src/main.py"]
