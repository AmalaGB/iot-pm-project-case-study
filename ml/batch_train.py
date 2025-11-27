import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib
import os

# Load dataset - adapt to the dataset's columns
df = pd.read_csv("../data/predictive_maintenance.csv")  # adjust filename

# simple feature engineering:
# assume df has columns: machine_id, temperature, vibration, rpm, label (0 ok, 1 fail soon)
features = ["temperature", "vibration", "rpm"]
df = df.dropna(subset=features + ["label"])
X = df[features]
y = df["label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)
pred = clf.predict(X_test)
print(classification_report(y_test, pred))

os.makedirs("../models", exist_ok=True)
joblib.dump(clf, "../models/rf_pm_v1.joblib")
print("Model saved to ../models/rf_pm_v1.joblib")
