import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader


class WeatherModel(nn.Module):
    def __init__(self, input_size):
        super(WeatherModel, self).__init__()
        
        
        self.fc1 = nn.Linear(input_size, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 16)
        self.fc4 = nn.Linear(16, 1)
    
    def forward(self, x): 
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = F.relu(self.fc3(x))
        x = self.fc4(x)
        return x
# end of Weather Model class

class WeatherDataset(Dataset):
    def __init__(self, features, targets):
        self.features = torch.tensor(features.values, dtype=torch.float32)
        self.targets = torch.tensor(targets.values, dtype=torch.float32).view(-1,1)
    
    def __len__(self):
        return len(self.features)
    
    def __getitem__(self, index):
        # feature_tensor = torch.tensor(self.features.iloc[index].values, dtype=torch.float32)
        # target_tensor = torch.tensor(self.targets.iloc[index].values, dtype=torch.float32)
        return self.features[index], self.targets[index]
# end of WeatherDataset class

def prep_data(data, target_column):
    features = data.drop(columns=[target_column])
    targets = data[target_column]
    X_train, X_val, Y_train, Y_val = train_test_split (features, targets, test_size=0.2, random_state=42) # 80 percent train, 20 percent validation
    return X_train, X_val, Y_train, Y_val

def train_model(train_features, validation_features, train_targets, validation_targets):
    model = WeatherModel(input_size=train_features.shape[1])
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.0002)
    
    train_dataset = WeatherDataset(train_features, train_targets)
    validation_dataset = WeatherDataset(validation_features, validation_targets)
    
    train_loader = DataLoader(train_dataset, batch_size=16, shuffle=True)
    validation_loader = DataLoader(validation_dataset, batch_size=16, shuffle=False)
    
    epochs = 300
    final_train_loss = 0
    final_val_loss = 0
    
    for epoch in range(epochs):
        model.train()
        train_loss = 0
        for batch_features, batch_targets in train_loader:
            optimizer.zero_grad()
            predictions = model(batch_features)
            loss = criterion(predictions, batch_targets)
            loss.backward()
            optimizer.step()
            train_loss += loss.item()
            
        # Store final training loss for the last epoch
        final_train_loss = train_loss / len(train_loader)
        
        model.eval()
        val_loss = 0
        
        with torch.no_grad():
            for batch_features, batch_targets in validation_loader:
                predictions = model(batch_features)
                loss = criterion(predictions, batch_targets)
                val_loss += loss.item()
                
        # Store final validation loss
        final_val_loss = val_loss / len(validation_loader)
        
        print(f"Epoch {epoch+1} / {epochs} | Train Loss: {train_loss/len(train_loader):.4f}| Val Loss: {val_loss/len(validation_loader):.4f}")
    
    print(f"Final Training Loss: {final_train_loss:.4f}")
    print(f"Final Validation Loss: {final_val_loss:.4f}")
    
    relative_loss_train = final_train_loss / train_targets.mean()
    relative_loss_val = final_val_loss / validation_targets.mean()
    print(f"Relative Train Loss: {relative_loss_train:.4f}")
    print(f"Relative Validation Loss: {relative_loss_val:.4f}")


def main():
    data_file_path = "../pytorch_practice/cleaned_weather_data.csv"
    data = pd.read_csv(data_file_path)
    target_columns = ["Fastest_5Sec_Wind_Direction", "Fastest_5Sec_Wind_Speed"]
        # seperating the missing data from the trainable data
    data_available = data.dropna(subset=target_columns) 
    data_missing_0 = data[data[target_columns[0]].isna()] # rows of missing data
    data_missing_1 = data[data[target_columns[1]].isna()] 
    
    dropped_data = data_available.drop(columns=[target_columns[1]]) # dropping target column 1 before getting data for target column 0
    train_features, validation_features, train_targets, validation_targets = prep_data(dropped_data, target_columns[0])

    print(f"Print Correlation based on R score: \n{train_features.corrwith(train_targets)}")
    
    train_model(train_features, validation_features, train_targets, validation_targets)
    
    
if __name__ == "__main__":
    main()