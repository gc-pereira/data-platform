import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *



class QualityChecks:
    def __init__(self, df, rules, glue_context, spark_session):
        self.df = df
        self.rules = rules
        self.glue_context = glue_context
        self.spark_session = spark_session

    def evaluate(self):
        dyf = DynamicFrame.fromDF(self.df, self.glue_context, "dyf") 
        EvaluateDataQualityMultiframe = EvaluateDataQuality.process_rows(
            frame=dyf,
            ruleset=self.rules,
            publishing_options={
                "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe",
                "enableDataQualityCloudWatchMetrics": True,
                "enableDataQualityResultsPublishing": True,
            },
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
        ) 

        ruleOutcomes = SelectFromCollection.apply(
            dfc=EvaluateDataQualityMultiframe,
            key="ruleOutcomes",
            transformation_ctx="ruleOutcomes",
        ).toDF()

        return ruleOutcomes, ruleOutcomes.filter("Outcome == 'Failed'").count() == 0                         
                