FROM 007465388569.dkr.ecr.ap-northeast-1.amazonaws.com/genbato-cms-ecr-lambda/aws-parameters-and-secrets-lambda-extension:v16-x86_64 AS layer
FROM public.ecr.aws/lambda/python:3.11

RUN yum -y update glib2
RUN yum -y update nghttp2
RUN yum -y update libnghttp2
RUN yum -y update glibc-all-langpacks
RUN yum -y update glibc-minimal-langpack
RUN yum -y update glibc-common
RUN yum -y update expat
RUN yum -y update libxml2
RUN yum -y update freetype
RUN yum -y update libcap

RUN pip install --upgrade setuptools

COPY requirements.txt ./
COPY s3_import_opensaerch.py ./

RUN pip install -r requirements.txt

WORKDIR /opt 
COPY --from=layer /opt/ . 

CMD ["s3_import_opensaerch.lambda_handler"]