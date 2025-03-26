FROM python:3.12-slim

WORKDIR /application

RUN pip install -q poetry && \
  poetry config virtualenvs.create false

COPY poetry.lock pyproject.toml /application/
RUN poetry install -n --no-root

COPY app /application/app
RUN rm /application/app/consume.py

COPY app/consume.py /application/

ENTRYPOINT ["poetry", "run", "python", "consume.py"]