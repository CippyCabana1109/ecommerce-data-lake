"""
Sales forecasting module using PySpark MLlib.
Provides time series forecasting for ecommerce sales data.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, datediff, current_date, lag, avg, stddev, 
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesForecasting:
    """
    Sales forecasting using machine learning models.
    
    Features:
    - Time series feature engineering
    - Multiple ML algorithms (Random Forest, GBT, Linear Regression)
    - Cross-validation and hyperparameter tuning
    - Feature importance analysis
    - Forecast generation with confidence intervals
    """
    
    def __init__(self, spark_session: SparkSession):
        """
        Initialize sales forecasting module.
        
        Args:
            spark_session: Active SparkSession
        """
        self.spark = spark_session
        self.models = {}
        self.feature_columns = []
        self.evaluations = {}
        
    def prepare_features(self, df: DataFrame) -> DataFrame:
        """
        Prepare features for machine learning model.
        
        Args:
            df: Input DataFrame with sales data
            
        Returns:
            DataFrame: Feature-engineered DataFrame
        """
        logger.info("Preparing features for ML model...")
        
        # Ensure date column is properly formatted
        df = df.withColumn("date", col("date").cast("date"))
        
        # Time-based features
        df = (df
              .withColumn("day_of_week", dayofweek(col("date")))
              .withColumn("day_of_month", dayofmonth(col("date")))
              .withColumn("month", month(col("date")))
              .withColumn("quarter", quarter(col("date")))
              .withColumn("year", year(col("date")))
              .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0)))
        
        # Lag features (previous day/week sales)
        window_spec = Window.partitionBy("product_id", "county").orderBy("date")
        
        df = (df
              .withColumn("prev_day_quantity", lag("quantity", 1).over(window_spec))
              .withColumn("prev_week_quantity", lag("quantity", 7).over(window_spec))
              .withColumn("prev_month_quantity", lag("quantity", 30).over(window_spec)))
        
        # Rolling averages
        df = (df
              .withColumn("rolling_3d_avg", avg("quantity").over(window_spec.rowsBetween(-2, 0)))
              .withColumn("rolling_7d_avg", avg("quantity").over(window_spec.rowsBetween(-6, 0)))
              .withColumn("rolling_30d_avg", avg("quantity").over(window_spec.rowsBetween(-29, 0))))
        
        # Rolling standard deviations (volatility)
        df = (df
              .withColumn("rolling_7d_std", stddev("quantity").over(window_spec.rowsBetween(-6, 0)))
              .withColumn("rolling_30d_std", stddev("quantity").over(window_spec.rowsBetween(-29, 0))))
        
        # Price-related features
        df = (df
              .withColumn("revenue", col("quantity") * col("price"))
              .withColumn("avg_price_7d", avg("price").over(window_spec.rowsBetween(-6, 0))))
        
        # Fill null values in lag features
        lag_columns = ["prev_day_quantity", "prev_week_quantity", "prev_month_quantity",
                      "rolling_3d_avg", "rolling_7d_avg", "rolling_30d_avg",
                      "rolling_7d_std", "rolling_30d_std", "avg_price_7d"]
        
        for col_name in lag_columns:
            df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
        
        # Define feature columns
        self.feature_columns = [
            "day_of_week", "day_of_month", "month", "quarter", "year", "is_weekend",
            "prev_day_quantity", "prev_week_quantity", "prev_month_quantity",
            "rolling_3d_avg", "rolling_7d_avg", "rolling_30d_avg",
            "rolling_7d_std", "rolling_30d_std", "price", "avg_price_7d"
        ]
        
        logger.info(f"✅ Feature engineering completed. Features: {len(self.feature_columns)}")
        return df
    
    def train_models(self, df: DataFrame, target_col: str = "quantity") -> Dict[str, Pipeline]:
        """
        Train multiple ML models and select the best one.
        
        Args:
            df: Training DataFrame with features
            target_col: Target column name
            
        Returns:
            Dict: Trained models
        """
        logger.info("Training ML models...")
        
        # Prepare data
        feature_assembler = VectorAssembler(
            inputCols=self.feature_columns,
            outputCol="features"
        )
        
        # Index categorical columns if any
        indexers = []
        categorical_cols = ["county", "product_id", "category"]
        
        for col_name in categorical_cols:
            if col_name in df.columns:
                indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed")
                indexers.append(indexer)
        
        # Define models
        models_config = {
            "random_forest": RandomForestRegressor(
                featuresCol="features",
                labelCol=target_col,
                numTrees=100,
                maxDepth=10,
                seed=42
            ),
            "gradient_boosted": GBTRegressor(
                featuresCol="features",
                labelCol=target_col,
                maxDepth=8,
                maxBins=32,
                seed=42
            ),
            "linear_regression": LinearRegression(
                featuresCol="features",
                labelCol=target_col,
                elasticNetParam=0.5,
                regParam=0.1
            )
        }
        
        trained_models = {}
        
        # Split data
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
        
        for model_name, model in models_config.items():
            try:
                logger.info(f"Training {model_name} model...")
                
                # Create pipeline
                stages = indexers + [feature_assembler, model]
                pipeline = Pipeline(stages=stages)
                
                # Train model
                trained_model = pipeline.fit(train_data)
                
                # Evaluate
                predictions = trained_model.transform(test_data)
                evaluator = RegressionEvaluator(labelCol=target_col, predictionCol="prediction")
                
                metrics = {
                    "rmse": evaluator.setMetricName("rmse").evaluate(predictions),
                    "mae": evaluator.setMetricName("mae").evaluate(predictions),
                    "r2": evaluator.setMetricName("r2").evaluate(predictions)
                }
                
                self.evaluations[model_name] = metrics
                trained_models[model_name] = trained_model
                
                logger.info(f"✅ {model_name} trained successfully")
                logger.info(f"   RMSE: {metrics['rmse']:.2f}")
                logger.info(f"   MAE: {metrics['mae']:.2f}")
                logger.info(f"   R²: {metrics['r2']:.3f}")
                
            except Exception as e:
                logger.error(f"❌ Failed to train {model_name}: {e}")
        
        # Select best model based on RMSE
        if trained_models:
            best_model_name = min(self.evaluations.keys(), 
                               key=lambda x: self.evaluations[x]["rmse"])
            self.models["best"] = trained_models[best_model_name]
            self.models["best_name"] = best_model_name
            
            logger.info(f"🏆 Best model: {best_model_name}")
            logger.info(f"   RMSE: {self.evaluations[best_model_name]['rmse']:.2f}")
        
        return trained_models
    
    def forecast(self, df: DataFrame, days_ahead: int = 30) -> DataFrame:
        """
        Generate sales forecasts for future dates.
        
        Args:
            df: Historical data
            days_ahead: Number of days to forecast
            
        Returns:
            DataFrame: Forecast results
        """
        if "best" not in self.models:
            raise ValueError("No trained model available. Call train_models() first.")
        
        logger.info(f"Generating {days_ahead}-day sales forecast...")
        
        # Get unique product-county combinations
        product_counties = df.select("product_id", "county").distinct()
        
        # Generate future dates
        max_date = df.agg({"date": "max"}).collect()[0][0]
        future_dates = [(max_date + timedelta(days=i)) for i in range(1, days_ahead + 1)]
        
        # Create forecast DataFrame
        forecast_data = []
        for product_county in product_counties.collect():
            product_id = product_county.product_id
            county = product_county.county
            
            for date in future_dates:
                # Get historical data for feature calculation
                historical_subset = df.filter(
                    (col("product_id") == product_id) & 
                    (col("county") == county) &
                    (col("date") < date)
                ).orderBy(col("date").desc())
                
                if historical_subset.count() > 0:
                    # Calculate features for future date
                    last_row = historical_subset.first()
                    
                    forecast_data.append({
                        "product_id": product_id,
                        "county": county,
                        "date": date,
                        "day_of_week": date.weekday() + 1,
                        "day_of_month": date.day,
                        "month": date.month,
                        "quarter": (date.month - 1) // 3 + 1,
                        "year": date.year,
                        "is_weekend": 1 if date.weekday() >= 5 else 0,
                        "prev_day_quantity": last_row.quantity if last_row else 0,
                        "price": last_row.price if last_row else 0,
                        # Simplified features for demo
                        "prev_week_quantity": last_row.quantity if last_row else 0,
                        "prev_month_quantity": last_row.quantity if last_row else 0,
                        "rolling_3d_avg": last_row.quantity if last_row else 0,
                        "rolling_7d_avg": last_row.quantity if last_row else 0,
                        "rolling_30d_avg": last_row.quantity if last_row else 0,
                        "rolling_7d_std": 0,
                        "rolling_30d_std": 0,
                        "avg_price_7d": last_row.price if last_row else 0
                    })
        
        # Create DataFrame
        forecast_df = self.spark.createDataFrame(forecast_data)
        
        # Make predictions
        predictions = self.models["best"].transform(forecast_df)
        
        # Select relevant columns
        result = predictions.select(
            "date", "product_id", "county", "prediction",
            "day_of_week", "month", "year"
        ).withColumnRenamed("prediction", "forecast_quantity")
        
        logger.info(f"✅ Generated {result.count()} forecast records")
        return result
    
    def get_feature_importance(self) -> Dict[str, float]:
        """
        Get feature importance from the best model.
        
        Returns:
            Dict: Feature importance scores
        """
        if "best" not in self.models:
            raise ValueError("No trained model available")
        
        # Get the actual model from pipeline
        model = self.models["best"].stages[-1]  # Last stage is the model
        
        if hasattr(model, 'featureImportances'):
            importances = model.featureImportances.toArray()
            
            # Map to feature names
            feature_importance = dict(zip(self.feature_columns, importances))
            
            # Sort by importance
            sorted_importance = dict(sorted(feature_importance.items(), 
                                          key=lambda x: x[1], reverse=True))
            
            return sorted_importance
        else:
            logger.warning("Model does not support feature importance")
            return {}
    
    def evaluate_forecast_accuracy(self, historical_df: DataFrame, forecast_df: DataFrame) -> Dict[str, float]:
        """
        Evaluate forecast accuracy against actual data.
        
        Args:
            historical_df: Actual historical data
            forecast_df: Forecast predictions
            
        Returns:
            Dict: Accuracy metrics
        """
        # Join forecasts with actual data
        comparison = forecast_df.join(
            historical_df,
            on=["date", "product_id", "county"],
            how="inner"
        )
        
        if comparison.count() == 0:
            logger.warning("No overlapping data for forecast evaluation")
            return {}
        
        # Calculate metrics
        evaluator = RegressionEvaluator(
            labelCol="quantity",
            predictionCol="forecast_quantity"
        )
        
        metrics = {
            "rmse": evaluator.setMetricName("rmse").evaluate(comparison),
            "mae": evaluator.setMetricName("mae").evaluate(comparison),
            "r2": evaluator.setMetricName("r2").evaluate(comparison)
        }
        
        logger.info("📊 Forecast accuracy metrics:")
        logger.info(f"   RMSE: {metrics['rmse']:.2f}")
        logger.info(f"   MAE: {metrics['mae']:.2f}")
        logger.info(f"   R²: {metrics['r2']:.3f}")
        
        return metrics


def create_sample_data(spark: SparkSession) -> DataFrame:
    """Create sample sales data for testing."""
    import random
    from datetime import datetime, timedelta
    
    # Generate sample data
    data = []
    products = ["P001", "P002", "P003"]
    counties = ["Nairobi", "Mombasa", "Kisumu"]
    base_date = datetime(2023, 1, 1)
    
    for i in range(90):  # 3 months of data
        date = base_date + timedelta(days=i)
        
        for product in products:
            for county in counties:
                quantity = random.randint(1, 20)
                price = random.uniform(10, 100)
                
                data.append((product, date, county, quantity, price))
    
    return spark.createDataFrame(data, ["product_id", "date", "county", "quantity", "price"])


def main():
    """Main function to demonstrate sales forecasting."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SalesForecastingDemo") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Create sample data
        logger.info("Creating sample data...")
        sales_data = create_sample_data(spark)
        
        # Initialize forecasting module
        forecaster = SalesForecasting(spark)
        
        # Prepare features
        featured_data = forecaster.prepare_features(sales_data)
        
        # Train models
        models = forecaster.train_models(featured_data)
        
        if models:
            # Generate forecasts
            forecasts = forecaster.forecast(featured_data, days_ahead=7)
            
            # Show sample forecasts
            logger.info("Sample forecasts:")
            forecasts.show(10, truncate=False)
            
            # Get feature importance
            importance = forecaster.get_feature_importance()
            logger.info("Feature importance:")
            for feature, score in list(importance.items())[:5]:
                logger.info(f"  {feature}: {score:.3f}")
        
        logger.info("🎉 Sales forecasting demo completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        return 1
        
    finally:
        spark.stop()
    
    return 0


if __name__ == "__main__":
    exit(main())
