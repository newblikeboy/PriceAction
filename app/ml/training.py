from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from xgboost import XGBClassifier

from app.storage.database import Database


class MLFeatureExporter:
    def __init__(self, database: Database | None = None) -> None:
        self.database = database or Database()

    def load_features(self) -> pd.DataFrame:
        with self.database.connect() as db:
            with db.cursor() as cursor:
                cursor.execute("SELECT features_json FROM ml_features ORDER BY id")
                rows = cursor.fetchall()
        return pd.DataFrame([json.loads(row["features_json"]) for row in rows])

    def export_csv(self, path: str | Path) -> Path:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        self.load_features().to_csv(output, index=False)
        return output


class XGBoostTrainer:
    """Placeholder trainer for later stages after enough paper trades exist."""

    def train_result_classifier(self, frame: pd.DataFrame) -> XGBClassifier:
        if "result" not in frame.columns:
            raise ValueError("ML dataset must include result labels before training")
        features = frame.drop(columns=["result"]).select_dtypes(include=["number", "bool"]).fillna(0)
        labels = frame["result"].map({"LOSS": 0, "FLAT": 1, "WIN": 2})
        model = XGBClassifier(
            objective="multi:softprob",
            eval_metric="mlogloss",
            n_estimators=100,
            max_depth=3,
            learning_rate=0.05,
        )
        model.fit(features, labels)
        return model
