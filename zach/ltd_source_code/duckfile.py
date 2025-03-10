#-----------
# Author: Zach Benedetti
# Date: 3/9/25
# Purpose of File: Create a ML model to compute feature importance in batches
#-----------
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error

# Connect to the DuckDB database
con = duckdb.connect("new_database.db")
query = "SELECT * FROM aggregated_data LIMIT 1000"  # Limiting to the first 1000 rows for analysis
df = con.execute(query).fetchdf()

# Print column names
print(df.columns)

if 'calendar_date' in df.columns:
    
    df['calendar_date'] = pd.to_datetime(df['calendar_date'])
    df['hour'] = df['calendar_date'].dt.hour
    df['day_of_week'] = df['calendar_date'].dt.dayofweek
    df = df.drop(columns=['calendar_date'])

# Drop rows with missing values
df = df.dropna()

# Drop columns that are non-numeric
df = df.select_dtypes(include=['number']) 

# Ridership
target_column = 'board'

# Split data into features (x) and target (y)
X = df.drop(columns=[target_column])
y = df[target_column]

# Split into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize the features for linear model
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Create and fit a Linear Regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions
y_pred = model.predict(X_test)

# Calculate MSE
mse = mean_squared_error(y_test, y_pred)
print(f'Mean Squared Error: {mse}')

# Correlation Matrix
correlation_matrix = df.corr()
print(correlation_matrix)

# Plot the correlations of features with the target of ridership
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
plt.title('Correlation Matrix of Features with Ridership')
plt.show()

# Plot the actual vs predicted values (x for actual, y for predicted)
plt.figure(figsize=(10, 6))
plt.scatter(y_test, y_pred, alpha=0.5)
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
plt.xlabel('Actual Board')
plt.ylabel('Predicted Board')
plt.title('Actual vs Predicted Board')
plt.show()

# Plot the feature importances
coefficients = model.coef_
features = X.columns
feature_importances = pd.DataFrame(coefficients, index=features, columns=["importance"])
feature_importances = feature_importances.sort_values(by="importance", ascending=False)

# Plot feature importances
plt.figure(figsize=(10, 6))
feature_importances.plot(kind='bar')
plt.title('Feature Importances from Linear Regression Model')
plt.xlabel('Feature')
plt.ylabel('Importance')
plt.show()

# Close connection
con.close()
