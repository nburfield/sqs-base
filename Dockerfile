# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements and install dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY consumer.py ${LAMBDA_TASK_ROOT}/
COPY accountemail.html.j2 ${LAMBDA_TASK_ROOT}/
COPY accountemail.mjml.j2 ${LAMBDA_TASK_ROOT}/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "consumer.lambda_handler" ]
