FROM public.ecr.aws/lambda/python:3.11

# Install dependencies
RUN pip install --upgrade pip
RUN pip install pandas psycopg2-binary scipy --target .

# Copy function code
COPY lambda_function.py ./

# Command for Lambda
CMD ["lambda_function.lambda_handler"]
