#!/usr/bin/env python3
"""Local ML demo with synthetic UNSW-NB15 data."""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report
from sklearn.cluster import KMeans
import json

# Load synthetic training data
df = pd.read_csv('data/UNSW_NB15_training-set.csv')
print(f'Training data: {df.shape}')
print(f'Labels distribution: {df["label"].value_counts().to_dict()}')

# Identify feature columns (exclude id, label, attack_cat)
feature_cols = [c for c in df.columns if c not in ['id', 'label', 'attack_cat']]

# Encode categorical features (proto, service, state) - label encode for simplicity
df_encoded = df.copy()
for c in ['proto', 'service', 'state']:
    le = LabelEncoder()
    df_encoded[c] = le.fit_transform(df_encoded[c].astype(str))

# Features and labels
X = df_encoded[feature_cols].fillna(0).values
y = df['label'].values

# Encode labels
le_label = LabelEncoder()
y_enc = le_label.fit_transform(y)

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y_enc, test_size=0.2, random_state=42, stratify=y_enc)

# Train RandomForest
rf = RandomForestClassifier(n_estimators=100, max_depth=12, random_state=42)
rf.fit(X_train, y_train)

# Evaluate
y_pred = rf.predict(X_test)
print('\nRandom Forest Classification Report:')
print(classification_report(y_test, y_pred, target_names=['Normal', 'Attack']))

# Feature importances
importances = list(zip(feature_cols, rf.feature_importances_))
top_features = sorted(importances, key=lambda x: -x[1])[:15]
print('\nTop 15 features:')
for name, imp in top_features:
    print(f'  {name:30s} {imp:.4f}')

# KMeans anomaly detection (on normal data only)
normal_mask = y_enc == 0
normal_data = X[normal_mask]

kmeans = KMeans(n_clusters=8, random_state=42, n_init=10)
kmeans.fit(normal_data)

# Compute distances to assigned centroid
distances = []
for i in range(len(normal_data)):
    centroid_idx = kmeans.predict([normal_data[i]])[0]
    dist = np.sqrt(np.sum((normal_data[i] - kmeans.cluster_centers_[centroid_idx]) ** 2))
    distances.append(dist)

threshold = np.percentile(distances, 99)
print(f'\nKMeans (k=8) - 99th percentile threshold: {threshold:.4f}')

# Load test data and evaluate
df_test = pd.read_csv('data/UNSW_NB15_testing-set.csv')
df_test_encoded = df_test.copy()
for c in ['proto', 'service', 'state']:
    le_te = LabelEncoder()
    # Use the same classes as training if possible, otherwise fit on test
    try:
        df_test_encoded[c] = le_te.fit_transform(df_test_encoded[c].astype(str))
    except:
        pass

X_test2 = df_test_encoded[feature_cols].fillna(0).values
y_test2 = le_label.transform(df_test['label'].values)
preds = rf.predict(X_test2)

# Compute test distances
normal_test_mask = y_test2 == 0
normal_test_data = X_test2[normal_test_mask]
test_distances = []
for i in range(len(normal_test_data)):
    centroid_idx = kmeans.predict([normal_test_data[i]])[0]
    dist = np.sqrt(np.sum((normal_test_data[i] - kmeans.cluster_centers_[centroid_idx]) ** 2))
    test_distances.append(dist)

test_threshold = np.percentile(test_distances, 99) if len(test_distances) > 0 else threshold

# Severity counting
high = 0
medium = 0
none = 0

for i in range(len(X_test2)):
    classifier_fires = preds[i] == 1
    is_outlier = False
    if normal_test_mask[i]:
        d_idx = sum(normal_test_mask[:i]) if i < len(normal_test_mask) else 0
        if d_idx < len(test_distances):
            is_outlier = test_distances[d_idx] > test_threshold

    if classifier_fires and is_outlier:
        high += 1
    elif classifier_fires != is_outlier:
        medium += 1
    else:
        none += 1

print(f'\nSeverity (test set): high={high}, medium={medium}, none={none}')
print(f'Total: {high+medium+none} samples')

# Save metrics
metrics = {
    "rf_f1_macro": float(np.mean(classification_report(y_test, y_pred, output_dict=True, zero_division=0).get('macro avg', {}).get('f1', 0))),
    "kmeans_threshold": float(test_threshold),
    "kmeans_threshold_train": float(threshold),
}
with open('local_metrics.json', 'w') as f:
    json.dump(metrics, f, indent=2)
print(f'\nMetrics saved to local_metrics.json')