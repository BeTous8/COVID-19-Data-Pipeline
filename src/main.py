"""
COVID-19 Data Pipeline - Main Orchestrator
Runs the complete ETL pipeline: Extract -> Transform -> Validate -> Load
"""

import sys
from datetime import datetime

from extract import CovidDataExtractor
from transform import CovidDataTransformer
from data_quality import validate_data
from load import CovidDataLoader


def run_pipeline():
    """Execute the complete ETL pipeline"""
    print("=" * 60)
    print("COVID-19 DATA PIPELINE - STARTING")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        # Step 1: Extract
        print("\n[1/4] EXTRACTING DATA...")
        extractor = CovidDataExtractor()
        files = extractor.extract_all()
        print(f"[OK] Extracted {len(files)} files")

        # Step 2: Transform
        print("\n[2/4] TRANSFORMING DATA...")
        transformer = CovidDataTransformer()
        df = transformer.transform_all()

        # Step 3: Data Quality
        print("\n[3/4] RUNNING DATA QUALITY CHECKS...")
        passed, errors, warnings = validate_data(df)

        if not passed:
            print("\n[FAILED] Pipeline aborted due to data quality issues")
            return False

        # Step 4: Load
        print("\n[4/4] LOADING INTO DATABASE...")
        loader = CovidDataLoader()
        loader.connect()

        # Create run metadata
        run_id = loader.create_run_metadata(
            "https://github.com/CSSEGISandData/COVID-19"
        )

        # Load data
        records_loaded = loader.load_data(df, run_id)

        # Update metadata
        loader.update_metadata(run_id, records_loaded, 'SUCCESS')

        # Get final stats
        stats = loader.get_latest_stats()

        loader.close()

        # Print summary
        print("\n" + "=" * 60)
        print("[SUCCESS] PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"   Records processed: {records_loaded:,}")
        print(f"   Run ID: {run_id}")
        if stats:
            print(f"   Countries: {stats['countries']}")
            print(f"   Date range: {stats['earliest_date']} to {stats['latest_date']}")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n[FAILED] Pipeline failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
