import time
import json
import random
import os
from datetime import datetime
from faker import Faker

fake = Faker()

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

def generate_attendance_event():
    status_options = ['Present'] * 80 + ['Absent'] * 10 + ['Late'] * 10
    return {
        "event_id": fake.uuid4(),
        "event_type": "attendance",
        "student_id": random.randint(1, 1000),
        "school_id": random.randint(1, 5),
        "timestamp": datetime.now().isoformat(),
        "status": random.choice(status_options)
    }

def generate_academic_event():
    subjects = ['Math', 'Science', 'English', 'History', 'Art']
    score = int(random.gauss(80, 10))
    score = max(0, min(100, score)) # clamp between 0 and 100
    
    return {
        "event_id": fake.uuid4(),
        "event_type": "academic",
        "student_id": random.randint(1, 1000),
        "school_id": random.randint(1, 5),
        "timestamp": datetime.now().isoformat(),
        "subject": random.choice(subjects),
        "score": score
    }

def generate_behavior_event():
    incidents = ['Disruption', 'Tardiness', 'Bullying', 'Vandalism']
    severities = ['Low', 'Medium', 'High']
    severity = random.choices(severities, weights=[70, 20, 10])[0]
    
    return {
        "event_id": fake.uuid4(),
        "event_type": "behavior",
        "student_id": random.randint(1, 1000),
        "school_id": random.randint(1, 5),
        "timestamp": datetime.now().isoformat(),
        "incident_type": random.choice(incidents),
        "severity": severity
    }

def main():
    print("Starting Real-Time Schooling Data Generator...")
    try:
        while True:
            events = []
            start_time = time.time()
            
            # Generate a batch of events
            for _ in range(random.randint(10, 50)):
                event_type = random.choices(
                    ['attendance', 'academic', 'behavior'], 
                    weights=[60, 30, 10]
                )[0]
                
                if event_type == 'attendance':
                    events.append(generate_attendance_event())
                elif event_type == 'academic':
                    events.append(generate_academic_event())
                else:
                    events.append(generate_behavior_event())
            
            # Write batch to JSONL file
            file_name = f"events_{int(start_time*1000)}.jsonl"
            file_path = os.path.join(RAW_DIR, file_name)
            
            with open(file_path, 'w') as f:
                for event in events:
                    f.write(json.dumps(event) + '\n')
            
            # Record health metrics
            latency_ms = (time.time() - start_time) * 1000
            health_event = {
                "event_id": fake.uuid4(),
                "event_type": "system_health",
                "timestamp": datetime.now().isoformat(),
                "component": "DataGenerator",
                "status": random.choices(['OK', 'Warning', 'Error'], weights=[95, 3, 2])[0],
                "latency_ms": latency_ms
            }
            with open(file_path, 'a') as f:
                 f.write(json.dumps(health_event) + '\n')
            
            print(f"Generated {len(events)} events -> {file_name}")
            time.sleep(2) # generate events every 2 seconds
            
    except KeyboardInterrupt:
        print("\nStopping data generator.")

if __name__ == "__main__":
    main()
