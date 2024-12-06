# train_xgboost.py
import pandas as pd
import xgboost as xgb
import joblib
import json
import matplotlib.pyplot as plt
import io
from google.cloud import storage
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score, confusion_matrix, roc_curve

def upload_to_gcs(content, gcs_path, bucket_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content)

def main():
    # Configuration
    bucket_name = "ml-experiment-tracker"
    data_path = "gs://temp_bucket_for_clean_data/best_features_for_churn.csv"
    evaluation_metrics_folder = "Evaluation_metrics"
    model_artifacts_folder = "model_artifacts"
    
    # Load data from GCS
    data = pd.read_csv(data_path)
    
    # Split into features and target
    X = data.drop(columns=["Churn"])
    y = data["Churn"]
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train XGBoost model
    model = xgb.XGBClassifier(use_label_encoder=False)
    model.fit(X_train, y_train)
    
    # Evaluate the model
    y_pred = model.predict(X_test)
    y_scores = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_scores)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)
    
    # Save metrics as JSON
    metrics = {
        "AUC": auc,
        "Precision": precision,
        "Recall": recall,
        "Confusion Matrix": conf_matrix.tolist(),
    }
    metrics_json = json.dumps(metrics)
    upload_to_gcs(metrics_json, f"{evaluation_metrics_folder}/metrics.json", bucket_name)

    # Plot confusion matrix
    conf_matrix_plot = io.BytesIO()
    plt.figure()
    plt.imshow(conf_matrix, interpolation='nearest', cmap=plt.cm.Blues)
    plt.colorbar()
    plt.xlabel("Predicted Label")
    plt.ylabel("True Label")
    plt.title("Confusion Matrix")
    plt.savefig(conf_matrix_plot, format='png')
    conf_matrix_plot.seek(0)
    upload_to_gcs(conf_matrix_plot.read(), f"{evaluation_metrics_folder}/confusion_matrix.png", bucket_name)
    
    # Plot ROC curve
    roc_curve_plot = io.BytesIO()
    fpr, tpr, _ = roc_curve(y_test, y_scores)
    plt.figure()
    plt.plot(fpr, tpr, color='blue', label=f'AUC = {auc:.2f}')
    plt.plot([0, 1], [0, 1], color='grey', linestyle='--')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curve')
    plt.legend(loc='lower right')
    plt.savefig(roc_curve_plot, format='png')
    roc_curve_plot.seek(0)
    upload_to_gcs(roc_curve_plot.read(), f"{evaluation_metrics_folder}/roc_curve.png", bucket_name)
    
    # Save the trained model
    model_file = io.BytesIO()
    joblib.dump(model, model_file)
    model_file.seek(0)
    upload_to_gcs(model_file.read(), f"{model_artifacts_folder}/trained_model.model", bucket_name)
    

    print("Model training, evaluation, and artifact upload complete.")

if __name__ == "__main__":
    main()