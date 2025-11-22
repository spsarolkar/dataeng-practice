package in.sunilsarolkar;

// 🔹 14. Streaming + Slowly Changing Dimensions (SCD Type 2)
// Scenario: Customer attributes change over time (address, tier), and you receive real-time transactions.

// Task:

// Ingest real-time transactions from Kafka

// Join with SCD Type 2 dimension table to enrich with current customer info

// Use last_update_time to filter latest record per customer

// Apply watermarking and save enriched events

// Involves: Window functions, streaming join, deduplication, watermark, state management

public class SlowlyChangingDimentions {
    public static void main(String[] args){
        
    }
}
