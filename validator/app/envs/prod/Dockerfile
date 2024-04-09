ARG BASE_IMAGE=python:3.11-slim


FROM $BASE_IMAGE AS base-image
LABEL builder=true

WORKDIR /root/src/

RUN pip3 install --no-cache-dir 'pdm>=2.12,<3'

RUN apt-get update && apt-get install -y git
COPY pyproject.toml pdm.lock ./
RUN pdm lock --check
COPY --from=compute-horde . /root/compute_horde

RUN pdm config python.use_venv False && \
  # compute-horde from path computes version from git tag which is unavailable here, 
  # hence we pass PDM_BUILD_SCM_VERSION=0 manually
  PDM_BUILD_SCM_VERSION=0 pdm sync --prod --group :all
RUN mkdir -p /opt/ && mv __pypackages__/3.11/ /opt/pypackages/
ENV PATH=/opt/pypackages/bin:$PATH
ENV PYTHONPATH=/opt/pypackages/lib:$PYTHONPATH

COPY ./app/src/ /root/src/
COPY ./app/envs/prod/entrypoint.sh /root/src/
COPY ./app/envs/prod/gunicorn.conf.py /root/src/
COPY ./app/envs/prod/celery-entrypoint.sh /root/src/
COPY ./app/envs/prod/celery-beat-entrypoint.sh /root/src/
COPY ./app/envs/prod/prometheus-cleanup.sh /root/src/

RUN python3 -m compileall -b -f -q /root/


FROM $BASE_IMAGE AS secondary-image
LABEL builder=false

RUN apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /root/src/
ENV PYTHONUNBUFFERED=1
ENV PATH=/opt/pypackages/bin:$PATH
ENV PYTHONPATH=/opt/pypackages/lib:$PYTHONPATH

COPY --from=base-image /root/src/ /root/src/
COPY --from=base-image /opt/pypackages/ /opt/pypackages/

EXPOSE 8000

CMD ["./entrypoint.sh"]
