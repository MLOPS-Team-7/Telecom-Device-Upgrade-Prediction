# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Install necessary packages for GCP integration
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    xgboost \
    google-cloud-storage \
    google-cloud-aiplatform \
    google-auth \
    gcsfs \
    fsspec \
    matplotlib

# Copy the local training script to the container
COPY train_xgboost.py /app/

# Run the script when the container starts
CMD ["python", "train_xgboost.py"]
