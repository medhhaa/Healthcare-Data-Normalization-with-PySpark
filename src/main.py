from data_processor import DataProcessor


def main():
    input_file = "./data/legacy_healthcare_data.csv"
    processor = DataProcessor(input_file)

    # === Step-by-step dimension creation ===
    dim_patient = processor.create_dim_patient()
    dim_insurance = processor.create_dim_insurance()
    dim_billing = processor.create_dim_billing()
    dim_provider = processor.create_dim_provider()
    dim_location = processor.create_dim_location()
    dim_primary_diag = processor.create_dim_primary_diagnosis()
    dim_secondary_diag = processor.create_dim_secondary_diagnosis()
    dim_treatment = processor.create_dim_treatment()
    dim_prescription = processor.create_dim_prescription()
    dim_lab_order = processor.create_dim_lab_order()

    # === Fact table creation ===
    print("Fact Table Creation")
    fact_visit = processor.create_fact_visit(
        dim_provider, dim_location, dim_primary_diag, dim_secondary_diag, dim_treatment
    )

    # === Run debug checks ===
    print("To Verify Normalization into SnowFlake Schema")
    processor.verify_fact_join_accuracy(fact_visit, dim_provider)
    processor.verify_diagnosis_joins(fact_visit, dim_primary_diag, dim_secondary_diag)
    processor.verify_location_join_accuracy(fact_visit, dim_location)
    processor.verify_treatment_join_accuracy(fact_visit, dim_treatment)
    processor.verify_data_completeness(fact_visit)


if __name__ == "__main__":
    main()
