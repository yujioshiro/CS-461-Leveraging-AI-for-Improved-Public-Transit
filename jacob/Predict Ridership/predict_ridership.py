import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt


# Data Loading and Preprocessing
class TransitDataset:
    """
    Convert the input data into a PyTorch Dataset
    """
    def __init__(self, data_path, test_size=0.2, random_state=42):
        self.df = pd.read_csv(data_path, sep='\t')

        # TODO: Move this to final_df.py
        # Would like data processing to be handled before entering the model
        # Convert DATE to datetime and extract features
        self.df['DATE'] = pd.to_datetime(self.df['DATE'])
        self.df['DayOfWeek'] = self.df['DATE'].dt.dayofweek
        self.df['Month'] = self.df['DATE'].dt.month
        self.df['Day'] = self.df['DATE'].dt.day

        # Split features and target
        self.X = self.df.drop(['DATE', 'total_board'], axis=1)
        self.y = self.df['total_board']

        # Identify numeric and categorical columns
        self.numeric_features = self.X.select_dtypes(
            include=['int64', 'float64']
        ).columns
        self.categorical_features = self.X.select_dtypes(
            include=['bool', 'object']
        ).columns

        # Preprocessing pipeline
        numeric_transformer = Pipeline(steps=[
            ('scaler', StandardScaler())
        ])
        categorical_transformer = Pipeline(steps=[
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ])
        self.preprocessor = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, self.numeric_features),
                ('cat', categorical_transformer, self.categorical_features)
            ])

        # Split train-test
        self.X_train, self.X_test, self.y_train, self.y_test = (
            train_test_split(
                self.X, self.y, test_size=test_size, random_state=random_state
            )
        )

        # Fit and transform the training data
        self.X_train_processed = self.preprocessor.fit_transform(self.X_train)
        self.X_test_processed = self.preprocessor.transform(self.X_test)

        # Convert to tensors
        self.X_train_tensor = torch.FloatTensor(self.X_train_processed)
        self.y_train_tensor = torch.FloatTensor(
            (self.y_train.values).reshape(-1, 1)
        )
        self.X_test_tensor = torch.FloatTensor(self.X_test_processed)
        self.y_test_tensor = torch.FloatTensor(
            (self.y_test.values).reshape(-1, 1)
            )

        # Get feature dimension after preprocessing
        self.input_dim = self.X_train_processed.shape[1]

    def get_train_data(self):
        return self.X_train_tensor, self.y_train_tensor

    def get_test_data(self):
        return self.X_test_tensor, self.y_test_tensor

    def get_input_dim(self):
        return self.input_dim


# Model Definition
class RidershipModel(nn.Module):
    def __init__(self, input_dim, hidden_layers=[128, 64]):
        super(RidershipModel, self).__init__()

        # Build network layers dynamically
        layers = []
        prev_dim = input_dim

        for hidden_dim in hidden_layers:
            layers.append(nn.Linear(prev_dim, hidden_dim))
            layers.append(nn.ReLU())
            layers.append(nn.BatchNorm1d(hidden_dim))
            layers.append(nn.Dropout(0.2))
            prev_dim = hidden_dim

        # Output layer
        layers.append(nn.Linear(prev_dim, 1))

        self.model = nn.Sequential(*layers)

    def forward(self, x):
        return self.model(x)


# Training and Evaluation Functions
def train_model(
        model, X_train, y_train, epochs=100, batch_size=32, learning_rate=0.001
):
    # Create DataLoader
    train_data = torch.utils.data.TensorDataset(X_train, y_train)
    train_loader = DataLoader(
        train_data, batch_size=batch_size, shuffle=True, drop_last=True
    )

    # Loss function and optimizer
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    epoch_losses = []

    # Training loop
    for epoch in range(epochs):
        model.train()
        running_loss = 0.0

        for batch_X, batch_y in train_loader:
            # Forward pass
            outputs = model(batch_X)
            loss = criterion(outputs, batch_y)

            # Backward pass and optimize
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            running_loss += loss.item()

        epoch_loss = running_loss / len(train_loader)
        epoch_losses.append(epoch_loss)

        if epoch % 10 == 0:
            print(f'Epoch {epoch}, Loss: {epoch_loss:.4f}')

    return epoch_losses


def evaluate_model(model, X_test, y_test):
    model.eval()
    with torch.no_grad():
        predictions = model(X_test)
        mse = nn.MSELoss()(predictions, y_test).item()
        rmse = np.sqrt(mse)

        # Calculate R^2
        y_mean = torch.mean(y_test)
        ss_tot = torch.sum((y_test - y_mean) ** 2)
        ss_res = torch.sum((y_test - predictions) ** 2)
        r2 = 1 - (ss_res / ss_tot)

    return {
        'MSE': mse,
        'RMSE': rmse,
        'R2': r2.item()
    }


# Feature Importance
def analyze_feature_importance(model, dataset):
    # Get the weights from the first layer
    first_layer_weights = (
        model.model[0].weight.data.abs().mean(dim=0).cpu().numpy()
    )

    try:
        # Try to get all feature names from the preprocessor
        all_feature_names = dataset.preprocessor.get_feature_names_out()
    except Exception as e:
        print(f"Error getting feature names: {e}")
        # Fallback: create generic feature names
        all_feature_names = [
            f"Feature_{i}" for i in range(len(first_layer_weights))
        ]

    # Create feature importance dataframe
    if len(all_feature_names) == len(first_layer_weights):
        importance_df = pd.DataFrame({
            'Feature': all_feature_names,
            'Importance': first_layer_weights
        })
        return importance_df.sort_values('Importance', ascending=False)
    else:
        print("Feature names don't match model weights dimensions")
        print(
            f"Features: {len(all_feature_names)}, "
            f"Weights: {len(first_layer_weights)}"
        )

        # Create generic feature names as fallback
        importance_df = pd.DataFrame({
            'Feature': [f"Feature_{i}" for i in range(
                len(first_layer_weights)
            )],
            'Importance': first_layer_weights
        })
        return importance_df.sort_values('Importance', ascending=False)


def main():
    data_path = "./data/complete_data.tsv"

    # Initialize dataset
    print("Loading and preprocessing data...")
    transit_data = TransitDataset(data_path)
    X_train, y_train = transit_data.get_train_data()
    X_test, y_test = transit_data.get_test_data()
    input_dim = transit_data.get_input_dim()

    print(f"Input dimension after preprocessing: {input_dim}")

    # Initialize model
    model = RidershipModel(input_dim)
    print("Model architecture:")
    print(model)

    # Train model
    print("Training model...")
    train_model(model, X_train, y_train, epochs=300)

    # Evaluate model
    print("Evaluating model...")
    metrics = evaluate_model(model, X_test, y_test)
    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    # Analyze feature importance
    print("Analyzing feature importance...")
    importance_df = analyze_feature_importance(model, transit_data)
    if importance_df is not None:
        print("Top 15 most important features:")
        print(importance_df.head(15))

        # Plot feature importance
        plt.figure(figsize=(12, 8))
        top_features = importance_df.head(15)
        plt.barh(top_features['Feature'], top_features['Importance'])
        plt.title('Top 15 Feature Importance')
        plt.xlabel('Importance')
        plt.tight_layout()
        plt.savefig('feature_importance.png')

    # Save model
    torch.save(model.state_dict(), 'transit_ridership_model.pth')
    print("Model saved to transit_ridership_model.pth")


if __name__ == "__main__":
    main()
