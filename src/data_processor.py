from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    when,
    max as spark_max,
    to_timestamp,
    monotonically_increasing_id,
)
import os
import glob
import shutil


class DataProcessor:

    def __init__(self, input_file):
        self.spark = SparkSession.builder.appName(
            "HealthcareDataNormalizer"
        ).getOrCreate()
        self.df = self.spark.read.option("header", True).csv(input_file)
        self.df = self.df.withColumn("visit_date", to_timestamp(col("visit_datetime")))

    def derive_patient_status(self):
        """Add 'Active'/'Inactive' status based on whether patient visited after 2021."""
        last_visits = self.df.groupBy("patient_id").agg(
            spark_max("visit_date").alias("last_visit")
        )
        status = last_visits.withColumn(
            "patient_status",
            when(col("last_visit") <= "2021-12-31", "Inactive").otherwise("Active"),
        )
        return status

    def create_dim_patient(self):
        """Creates DimPatient using existing patient_id and adds derived status."""
        status_df = self.derive_patient_status()
        patient_cols = [
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            "patient_date_of_birth",
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            "patient_zip",
            "patient_phone",
            "patient_email",
        ]
        ordered_cols = patient_cols + ["patient_status"]
        patient_df = self.df.select(*patient_cols).dropDuplicates(["patient_id"])
        result = patient_df.join(
            status_df.select("patient_id", "patient_status"),
            on="patient_id",
            how="left",
        )
        print("✅ Dimension Patient created:")
        result.select(*ordered_cols).show(5, truncate=False)
        self.save_to_csv(result.select(*ordered_cols), "./data/answers/", "DimPatient")
        return result.select(*ordered_cols)

    def create_dim_insurance(self):
        """Creates DimInsurance using existing insurance_id."""
        ordered_cols = []
        result = self.df.select(
            "insurance_id",
            "patient_id",
            "insurance_payer_name",
            "insurance_policy_number",
            "insurance_group_number",
            "insurance_plan_type",
        ).dropDuplicates(["insurance_id"])
        print("✅ Dimension Insurance created:")
        result.show(5, truncate=False)
        self.save_to_csv(result, "./data/answers/", "DimInsurance")
        return result

    def create_dim_billing(self):
        """Creates DimBilling using existing billing_id."""
        result = self.df.select(
            "billing_id",
            "insurance_id",
            "billing_total_charge",
            "billing_amount_paid",
            "billing_date",
            "billing_payment_status",
        ).dropDuplicates(["billing_id"])
        print("✅ Dimension Billing created:")
        result.show(5, truncate=False)
        self.save_to_csv(result, "./data/answers/", "DimBilling")
        return result

    def create_dim_provider(self):
        """Creates DimProvider and assigns provider_id using (doctor_name, doctor_title, doctor_department)."""
        df = (
            self.df.select("doctor_name", "doctor_title", "doctor_department")
            .dropDuplicates()
            .filter(
                (col("doctor_name").isNotNull())
                & (col("doctor_title").isNotNull())
                & (col("doctor_department").isNotNull())
            )
            .orderBy("doctor_title", "doctor_department")
        )
        df = df.withColumn(
            "provider_id", monotonically_increasing_id() + 1
        )  # unique ID per distinct doctor triplet
        df = df.select(
            "provider_id", "doctor_name", "doctor_title", "doctor_department"
        )
        print("✅ Dimension Provider created:")
        df.show(5, truncate=False)
        self.save_to_csv(df, "./data/answers/", "DimProvider")
        return df

    def create_dim_location(self):
        """Creates DimLocation and assigns location_id using (clinic_name, room_number)."""
        df = (
            self.df.select("clinic_name", "room_number")
            .dropDuplicates()
            .filter((col("clinic_name").isNotNull()) & (col("room_number").isNotNull()))
            .orderBy("clinic_name", "room_number")
        )
        df = df.withColumn(
            "location_id", monotonically_increasing_id() + 1
        )  # unique ID per unique clinic-room
        df = df.select("location_id", "clinic_name", "room_number")
        print("✅ Dimension Location created:")
        df.show(5, truncate=False)
        self.save_to_csv(df, "./data/answers/", "DimLocation")
        return df

    def create_dim_primary_diagnosis(self):
        """Creates DimPrimaryDiagnosis using unique pairs and adds ID."""
        df = (
            self.df.select("primary_diagnosis_code", "primary_diagnosis_desc")
            .dropDuplicates()
            .filter(
                (col("primary_diagnosis_code").isNotNull())
                & (col("primary_diagnosis_desc").isNotNull())
            )
        )
        df = df.withColumn(
            "primary_diagnosis_id", monotonically_increasing_id() + 1
        )  # based on unique (code, desc)
        df = df.select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )
        print("✅ Dimension PrimaryDiagnosis created:")
        df.show(5, truncate=False)
        self.save_to_csv(df, "./data/answers/", "DimPrimaryDiagnosis")
        return df

    def create_dim_secondary_diagnosis(self):
        """Creates DimSecondaryDiagnosis using unique pairs and adds ID."""
        df = (
            self.df.select("secondary_diagnosis_code", "secondary_diagnosis_desc")
            .dropDuplicates()
            .filter(
                (col("secondary_diagnosis_code").isNotNull())
                & (col("secondary_diagnosis_desc").isNotNull())
            )
        )
        df = df.withColumn(
            "secondary_diagnosis_id", monotonically_increasing_id() + 1
        )  # based on unique (code, desc)
        df = df.select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )
        print("✅ Dimension SecondaryDiagnosis created:")
        df.show(5, truncate=False)
        self.save_to_csv(df, "./data/answers/", "DimSecondaryDiagnosis")
        return df

    def create_dim_treatment(self):
        """Creates DimTreatment from treatment code/desc pair and assigns treatment_id."""
        df = (
            self.df.select("treatment_code", "treatment_desc")
            .dropDuplicates()
            .filter(
                (col("treatment_code").isNotNull())
                & (col("treatment_desc").isNotNull())
            )
        )
        df = df.withColumn(
            "treatment_id", monotonically_increasing_id() + 1
        )  # based on unique (code, desc)
        df = df.select("treatment_id", "treatment_code", "treatment_desc")
        print("✅ Dimension Treatment created:")
        df.show(5, truncate=False)
        self.save_to_csv(df, "./data/answers/", "DimTreatment")
        return df

    def create_dim_prescription(self):
        """Creates DimPrescription using existing prescription_id. Removes rows with null IDs.
        Creates DimPrescription using already existing prescription_id."""
        result = (
            self.df.select(
                "prescription_id",
                "prescription_drug_name",
                "prescription_dosage",
                "prescription_frequency",
                "prescription_duration_days",
            )
            .filter(col("prescription_id").isNotNull())
            .dropDuplicates(["prescription_id"])
        )
        print("✅ Dimension Prescription created:")
        result.show(5, truncate=False)
        self.save_to_csv(result, "./data/answers/", "DimPrescription")
        return result

    def create_dim_lab_order(self):
        """Creates DimLabOrder using existing lab_order_id. Removes rows with null IDs.
        Creates DimLabOrder using already existing lab_order_id."""
        result = (
            self.df.select(
                "lab_order_id",
                "lab_test_code",
                "lab_name",
                "lab_result_value",
                "lab_result_units",
                "lab_result_date",
            )
            .filter(col("lab_order_id").isNotNull())
            .dropDuplicates(["lab_order_id"])
        )
        print("✅ Dimension LabOrder created:")
        result.show(5, truncate=False)
        self.save_to_csv(result, "./data/answers/", "DimLabOrder")
        return result

    def create_fact_visit(
        self,
        dim_provider,
        dim_location,
        dim_primary_diag,
        dim_secondary_diag,
        dim_treatment,
    ):
        """
        Creates the FactVisit table by joining the legacy dataframe with dimension tables
        using matching business keys. All foreign keys are added after the joins.
        """
        fact = self.df
        # Join with each dimension using business key columns
        fact = fact.join(
            dim_provider,
            on=["doctor_name", "doctor_title", "doctor_department"],
            how="left",
        )
        fact = fact.join(dim_location, on=["clinic_name", "room_number"], how="left")
        fact = fact.join(
            dim_primary_diag,
            on=["primary_diagnosis_code", "primary_diagnosis_desc"],
            how="left",
        )
        fact = fact.join(
            dim_secondary_diag,
            on=["secondary_diagnosis_code", "secondary_diagnosis_desc"],
            how="left",
        )
        fact = fact.join(
            dim_treatment, on=["treatment_code", "treatment_desc"], how="left"
        )

        result = fact.select(
            "visit_id",
            "patient_id",
            "insurance_id",
            "billing_id",
            "location_id",
            "provider_id",
            "primary_diagnosis_id",
            "secondary_diagnosis_id",
            "treatment_id",
            "prescription_id",
            "lab_order_id",
            "visit_date",
            "visit_type",
        ).dropDuplicates(["visit_id"])

        print("✅ Fact Table Visit created:")
        result.show(5, truncate=False)
        self.save_to_csv(result, "./data/answers/", "FactVisit")
        return result

    def verify_fact_join_accuracy(self, fact_df, dim_provider):
        # The created Fact and Dimesion Tables:
        # Join fact and provider on provider_id
        joined_fact_and_dim = (
            fact_df.join(dim_provider, on="provider_id", how="left")
            .withColumnRenamed("doctor_name", "dim_doctor_name")
            .withColumnRenamed("doctor_title", "dim_doctor_title")
            .withColumnRenamed("doctor_department", "dim_doctor_department")
        )

        # Join back to original_df data to compare original doctor details
        joined_fact_w_original = joined_fact_and_dim.join(
            self.df.select(
                "visit_id", "doctor_name", "doctor_title", "doctor_department"
            ),
            on="visit_id",
            how="left",
        )

        # Compare values from dimension and fact
        mismatches = joined_fact_w_original.filter(
            (col("dim_doctor_name") != col("doctor_name"))
            | (col("dim_doctor_title") != col("doctor_title"))
            | (col("dim_doctor_department") != col("doctor_department"))
        )

        # Show only relevant columns for manual inspection
        print("Sample comparison (selected columns):")
        joined_fact_w_original.select(
            "visit_id",
            "provider_id",
            "dim_doctor_name",
            "doctor_name",
            "dim_doctor_title",
            "doctor_title",
            "dim_doctor_department",
            "doctor_department",
        ).show(10, truncate=False)
        # joined_fact_w_original.printSchema()

        total = joined_fact_w_original.count()
        wrong = mismatches.count()

        print(f"Total joined rows checked: {total}")
        print(f"Mismatched rows: {wrong}")
        if wrong > 0:
            mismatches.select(
                "visit_id", "provider_id", "dim_doctor_name", "doctor_name"
            ).show(truncate=False)

    def verify_diagnosis_joins(self, fact_df, dim_primary, dim_secondary):
        # Join with primary diagnosis dimension
        fact_w_primary_diagnosis = (
            fact_df.join(dim_primary, on="primary_diagnosis_id", how="left")
            .withColumnRenamed("primary_diagnosis_code", "dim_primary_code")
            .withColumnRenamed("primary_diagnosis_desc", "dim_primary_desc")
        )

        # Join with secondary diagnosis dimension
        fact_w_dim_diagnosis = (
            fact_w_primary_diagnosis.join(
                dim_secondary, on="secondary_diagnosis_id", how="left"
            )
            .withColumnRenamed("secondary_diagnosis_code", "dim_secondary_code")
            .withColumnRenamed("secondary_diagnosis_desc", "dim_secondary_desc")
        )

        # Join original_df with fact data
        joined_fact_w_original = fact_w_dim_diagnosis.join(
            self.df.select(
                "visit_id",
                "primary_diagnosis_code",
                "primary_diagnosis_desc",
                "secondary_diagnosis_code",
                "secondary_diagnosis_desc",
            ),
            on="visit_id",
            how="left",
        )

        # Compare original vs dimension-joined diagnosis
        mismatches = joined_fact_w_original.filter(
            (col("dim_primary_code") != col("primary_diagnosis_code"))
            | (col("dim_primary_desc") != col("primary_diagnosis_desc"))
            | (col("dim_secondary_code") != col("secondary_diagnosis_code"))
            | (col("dim_secondary_desc") != col("secondary_diagnosis_desc"))
        )

        print("Sample diagnosis join (selected columns):")
        joined_fact_w_original.select(
            "visit_id",
            "patient_id",
            "dim_primary_code",
            "primary_diagnosis_code",
            "dim_secondary_code",
            "secondary_diagnosis_code",
        ).show(10, truncate=False)

        total = joined_fact_w_original.count()
        wrong = mismatches.count()
        print(f"Diagnosis FK Verification: {total} rows checked")
        print(f"Diagnosis mismatches: {wrong}")
        if wrong > 0:
            mismatches.select(
                "visit_id",
                "dim_primary_code",
                "primary_diagnosis_code",
                "dim_secondary_code",
                "secondary_diagnosis_code",
            ).show(truncate=False)

    def verify_location_join_accuracy(self, fact_df, dim_location):
        joined_fact_and_dim = (
            fact_df.join(dim_location, on="location_id", how="left")
            .withColumnRenamed("clinic_name", "dim_clinic_name")
            .withColumnRenamed("room_number", "dim_room_number")
        )

        joined_fact_w_original = joined_fact_and_dim.join(
            self.df.select("visit_id", "clinic_name", "room_number"),
            on="visit_id",
            how="left",
        )

        mismatches = joined_fact_w_original.filter(
            (col("dim_clinic_name") != col("clinic_name"))
            | (col("dim_room_number") != col("room_number"))
        )

        total = joined_fact_w_original.count()
        wrong = mismatches.count()

        print("Location Join Sample:")
        joined_fact_w_original.select(
            "visit_id",
            "location_id",
            "dim_clinic_name",
            "clinic_name",
            "dim_room_number",
            "room_number",
        ).show(5, truncate=False)
        print(f"Total Checked: {total} | Mismatches: {wrong}")

        if wrong > 0:
            mismatches.select("visit_id", "dim_clinic_name", "clinic_name").show(
                truncate=False
            )

    def verify_data_completeness(self, fact_df):
        """
        Verify that the normalized schema captures all original visits.

        :param fact_df: Final fact table DataFrame
        """
        original_visits = self.df.select("visit_id").distinct().count()
        normalized_visits = fact_df.select("visit_id").distinct().count()

        if original_visits == normalized_visits:
            print(
                f"\nOriginal visits: {original_visits}, Normalized visits: {normalized_visits}"
            )
            print(
                f"Data completeness verified: All {original_visits} original visits preserved"
            )
        else:
            print(
                f"\nData completeness issue: Original visits: {original_visits}, Normalized visits: {normalized_visits}"
            )

            # Find missing visits
            missing = (
                self.df.select("visit_id")
                .distinct()
                .subtract(fact_df.select("visit_id"))
            )
            print(f"Missing {missing.count()} visits in normalized schema:")
            missing.show(10)

    def verify_treatment_join_accuracy(self, fact_df, dim_treatment):
        joined_fact_and_dim = (
            fact_df.join(dim_treatment, on="treatment_id", how="left")
            .withColumnRenamed("treatment_code", "dim_treatment_code")
            .withColumnRenamed("treatment_desc", "dim_treatment_desc")
        )

        joined_fact_w_original = joined_fact_and_dim.join(
            self.df.select("visit_id", "treatment_code", "treatment_desc"),
            on="visit_id",
            how="left",
        )

        mismatches = joined_fact_w_original.filter(
            (col("dim_treatment_code") != col("treatment_code"))
            | (col("dim_treatment_desc") != col("treatment_desc"))
        )

        total = joined_fact_w_original.count()
        wrong = mismatches.count()

        print("Treatment Join Sample:")
        joined_fact_w_original.select(
            "visit_id",
            "treatment_id",
            "dim_treatment_code",
            "treatment_code",
            "dim_treatment_desc",
            "treatment_desc",
        ).show(5, truncate=False)
        print(f"Total Checked: {total} | Mismatches: {wrong}")

        if wrong > 0:
            mismatches.select("visit_id", "dim_treatment_code", "treatment_code").show(
                truncate=False
            )

    def save_to_csv(self, df, output_path, filename) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename + ".csv")
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)
