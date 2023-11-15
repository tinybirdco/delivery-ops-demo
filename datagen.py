import os
import requests
import json
import faker
import logging
import datetime
from time import sleep

TARGET_ACTIVE_DELIVERIES = 10
MAX_ORDERS_PER_DELIVERY = 10
TARGET_AVAILABLE_CUSTOMERS = 50
TARGET_AVAILABLE_DRIVERS = 10
DELIVERY_TIME_INTERVAL = datetime.timedelta(seconds=15)
UPDATE_WAIT_SLEEP = 0.5
MINIMUM_TS_BOUNDARY = 0.01
DELIVERY_MISS_RATE = 0.05

fake = faker.Faker(locale='en_GB')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def current_time():
    """Returns the current time UTC"""
    # Using a function to prevent people not using UTC
    return datetime.datetime.utcnow()

def format_datetime(dt_obj):
    """Formats a datetime object into a string"""
    # Using a function to standardise the format
    return dt_obj.strftime('%Y-%m-%d %H:%M:%S.%f')

def parse_datetime(dt_str):
    """Parses a datetime string into a datetime object."""
    try:
        if '.' in dt_str and dt_str[-1] != 'Z':
            return datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
        elif '.' in dt_str and dt_str[-1] == 'Z':
            return datetime.datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            return datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')    
    except ValueError:
        logging.error(f"Error parsing datetime string {dt_str}")
        raise

def generate_unique_entity_id(existing_entities, id_field):
    """Generates a unique entity ID that is not in the existing_entities list."""
    existing_ids = {entity[id_field] for entity in existing_entities}
    while True:
        potential_id = fake.uuid4()
        if potential_id not in existing_ids:
            return potential_id

def generate_customers(existing_customers, count=10):
    new_customers = []
    for _ in range(count):
        customer = {
            'customer_id': generate_unique_entity_id(existing_customers, 'customer_id'),
            'name': fake.name(),
            'address': fake.address(),
            'updated_at': format_datetime(current_time())
        }
        new_customers.append(customer)
    return new_customers

def ensure_available_customers(tb_info):
    """Ensures that there are enough customers available to be assigned to new orders."""
    current_customers = fetch_tb_endpoint(tb_info, 'customers_by_order_status', {'status': ''})
    new_customer_count = TARGET_AVAILABLE_CUSTOMERS - len(current_customers)
    if new_customer_count > 0:
        logging.info(f"{len(current_customers)} customers found of target {TARGET_AVAILABLE_CUSTOMERS}; generating {new_customer_count} new customers...")
        new_customers = generate_customers(current_customers, new_customer_count)
        logging.info("Posting new customers to Tinybird...")
        post_data_to_tb(tb_info, 'cdc_customers', new_customers)
        wait_for_update(tb_info, new_customers[-1], 'customer_id', 'customers_api')
        return current_customers + new_customers
    else:
        logging.info(f"{len(current_customers)} customers found of target {TARGET_AVAILABLE_CUSTOMERS}; no new customers generated.")
        return current_customers

def generate_drivers(existing_drivers, count=5):
    new_drivers = []
    for _ in range(count):
        driver = {
            'driver_id': generate_unique_entity_id(existing_drivers, 'driver_id'),
            'name': fake.name(),
            'status': 'AVAILABLE',
            'updated_at': format_datetime(current_time())
        }
        new_drivers.append(driver)
    return new_drivers

def ensure_available_drivers(tb_info):
    current_drivers = fetch_tb_endpoint(tb_info, 'drivers_api', {'status_in': 'AVAILABLE'})
    new_driver_count = TARGET_AVAILABLE_DRIVERS - len(current_drivers)
    if new_driver_count > 0:
        logging.info(f"{len(current_drivers)} drivers found of target {TARGET_AVAILABLE_DRIVERS}; generating {new_driver_count} new drivers...")
        new_drivers = generate_drivers(current_drivers, new_driver_count)
        logging.info("Posting new drivers to Tinybird...")
        post_data_to_tb(tb_info, 'cdc_drivers', new_drivers)
        wait_for_update(tb_info, new_drivers[-1], 'driver_id', 'drivers_api')
        return current_drivers + new_drivers
    else:
        logging.info(f"{len(current_drivers)} drivers found of target {TARGET_AVAILABLE_DRIVERS}; no new drivers generated.")
    return current_drivers

def fetch_tb_endpoint(tb_info, endpoint, params=None):
    # Params are key:value dict, append to URI if present
    params = params if params else {}
    params['token'] = tb_info['token']
    try:
        resp = requests.get(
            f"{tb_info['host']}/v0/pipes/{endpoint}.json",
            params=params
        )
        resp.raise_for_status()
        if resp.status_code >= 200 and resp.status_code < 300:
            out = resp.json()['data']
            return out if out else []
        else:
            raise Exception(f"Error fetching Tinybird endpoint: {resp.status_code} with message {resp.text}")
    except requests.RequestException as e:
        logging.error(f"Error fetching Tinybird endpoint: {e}")
        raise

def post_data_to_tb(tb_info, table_name, data_batch):
    ndjson_data = '\n'.join([json.dumps(data) for data in data_batch])
    try:
        resp = requests.post(
            tb_info['host'] + f"/v0/events",
            params={'name': table_name, 'token': tb_info['token']},
            data=ndjson_data
        )
        resp.raise_for_status()
        logging.info(f"Posted {len(data_batch)} records to {table_name} table.")
        logging.debug(f"Tinybird response: {resp.text}")
    except requests.RequestException as e:
        logging.error(f"Error posting data to Tinybird: {e}")
        raise

def read_tb_info():
    try:
        with open(os.path.expanduser('.tinyb'), 'r') as f:
            tb_info = json.load(f)
        return tb_info
    except (IOError, json.JSONDecodeError) as e:
        logging.error(f"Error reading Tinybird info: {e}")
        raise

def generate_delivery(tb_info, orders=None):
    logging.info("Fetching Drivers without active deliveries from Tinybird...")
    drivers = ensure_available_drivers(tb_info)

    logging.info(f"Generating a new delivery and assigning a driver...")
    delivery_id = fake.uuid4()
    driver = drivers[0]
    num_orders = len(orders) if orders else fake.random_int(min=1, max=MAX_ORDERS_PER_DELIVERY)
    start_time = current_time()
    delivery = {
        "delivery_id": delivery_id,
        "driver_id": driver['driver_id'],
        "start_time": format_datetime(start_time),
        "status": "SCHEDULED",
        "updated_at": format_datetime(current_time())
    }
    # If rescheduling, use the provided orders
    if orders:
        new_orders = orders
    else:
        # Else generate new orders
        new_orders = []
        logging.info("Fetching Customers without active orders from Tinybird...")
        customers = ensure_available_customers(tb_info)
        logging.info("Generating new orders for delivery...")
        for _ in range(num_orders):
            new_orders.append({
                "order_id": fake.uuid4(),
                "customer_id": customers[_]['customer_id'],
                "delivery_completed_at": "",
            })
    # Now set all the other fields regardless
    for _ in range(num_orders):
        delivery_slot = start_time + _ * DELIVERY_TIME_INTERVAL
        new_orders[_]['delivery_id'] = delivery_id
        new_orders[_]['status'] = "SCHEDULED"
        new_orders[_]['updated_at'] = format_datetime(current_time())
        new_orders[_]['delivery_slot_start'] = format_datetime(delivery_slot)
        new_orders[_]['delivery_slot_end'] = format_datetime(delivery_slot + DELIVERY_TIME_INTERVAL)
        new_orders[_]['updated_at'] = format_datetime(current_time())

    
    delivery["orders"] = [order["order_id"] for order in new_orders]

    driver["status"] = "ASSIGNED"
    driver["updated_at"] = format_datetime(current_time())

    # Submit updates to Tinybird
    logging.info("Posting delivery to Tinybird...")
    post_data_to_tb(tb_info, "cdc_deliveries", [delivery])
    logging.info("Posting orders to Tinybird...")
    post_data_to_tb(tb_info, "cdc_orders", new_orders)
    logging.info("Posting driver assignment to Tinybird...")
    post_data_to_tb(tb_info, 'cdc_drivers', [driver])
    # Wait for update propagation
    wait_for_update(tb_info, delivery, 'delivery_id', 'deliveries_api')
    wait_for_update(tb_info, new_orders[-1], 'order_id', 'orders_api')
    wait_for_update(tb_info, driver, 'driver_id', 'drivers_api')
    logging.info("New delivery, orders and driver assignment propagated to Tinybird.")

def wait_for_update(tb_info, obj, status_field, endpoint, ts_field='updated_at'):
    obj_id = obj[status_field]
    expected_updated_at = parse_datetime(obj[ts_field])
    logging.info(f"Waiting for {status_field} {obj_id} to have '{ts_field}' >= {expected_updated_at}")

    while True:
        latest_record = fetch_tb_endpoint(tb_info, endpoint, {status_field: obj_id})
        if not latest_record:
            logging.info(f"{status_field} {obj_id} not found in endpoint {endpoint}. Waiting...")
            sleep(UPDATE_WAIT_SLEEP)
            continue

        latest_updated_at = parse_datetime(latest_record[0][ts_field])
        if latest_updated_at >= expected_updated_at:
            logging.info(f"{status_field} {obj_id} is up to date with '{ts_field}' = {latest_updated_at}")
            break
        else:
            logging.info(f"Latest '{ts_field}' {latest_updated_at} is before expected {expected_updated_at}. Waiting...")
            sleep(UPDATE_WAIT_SLEEP)

def ensure_active_deliveries(tb_info):
    logging.info("Fetching Current Deliveries from Tinybird...")
    active_deliveries = fetch_tb_endpoint(tb_info, 'deliveries_api', {'status': 'SCHEDULED'})
    current_delivery_count = len(active_deliveries)
    logging.info(f"{current_delivery_count} active deliveries found.")
    # If current deliveries is less than target, generate new deliveries
    if current_delivery_count < TARGET_ACTIVE_DELIVERIES:
        # Generate a new delivery and orders
        generate_delivery(tb_info)
    else:
        logging.info(f"Target active deliveries reached: {current_delivery_count}")

def send_driver_status_event(tb_info, driver_id, event_type, event_info=None):
    logging.info(f"Sending driver {driver_id} {event_type} event...")
    driver_event = {
        "driver_id": driver_id,
        "event_type": event_type,
        "event_info": event_info if event_info else "",
        "event_time": format_datetime(current_time())
    }
    post_data_to_tb(tb_info, 'driver_events', [driver_event])

def parse_delivery_slots(orders):
    # Filter orders by SCHEDULED status so we ignore completed orders
    scheduled_orders = [o for o in orders if o['status'] == 'SCHEDULED']
    active_slot_orders = [
        o for o in scheduled_orders 
        if parse_datetime(o['delivery_slot_start'])
        <= current_time() 
        <= parse_datetime(o['delivery_slot_end'])
    ]
    missed_slot_orders = [
        o for o in scheduled_orders 
        if current_time()
        > parse_datetime(o['delivery_slot_end'])
    ]
    pending_slot_orders = [
        o for o in scheduled_orders 
        if parse_datetime(o['delivery_slot_start'])
        > current_time()
    ]
    return active_slot_orders, missed_slot_orders, pending_slot_orders

def batch_update(tb_info, table_name, items, id_field, api_endpoint, ts_field='updated_at'):
    """
    Handles batch updates for a given table in Tinybird.

    Args:
    - tb_info (dict): Tinybird connection info.
    - table_name (str): The name of the table in Tinybird.
    - items (list): List of items to update.
    - id_field (str): The field name used as the identifier for wait_for_update.
    - api_endpoint (str): The API endpoint to check for update propagation.
    """
    if not items:
        logging.info(f"No items to batch update in table {table_name}.")
        return

    # Set updated_at for all items to current time
    for item in items:
        item[ts_field] = format_datetime(current_time())

    # Post batch update to Tinybird
    logging.info(f"Posting batch updates to {table_name} in Tinybird...")
    post_data_to_tb(tb_info, table_name, items)
    wait_for_update(tb_info, items[-1], id_field, api_endpoint, ts_field)

def process_delivery_status_updates(tb_info):
    order_delivery_status = fetch_tb_endpoint(tb_info, 'deliveries_by_order_status')
    if order_delivery_status:
        batch_update_deliveries = []
        batch_update_drivers = []
        for delivery_status in order_delivery_status:
            # Check if the delivery is in Scheduled status
            if delivery_status['delivery_status'] == 'SCHEDULED':
                delivery_id = delivery_status['delivery_id']
                # Fetch full delivery record
                delivery_info = fetch_tb_endpoint(tb_info, 'deliveries_api', {'delivery_id': delivery_id})[0]
                driver_id = delivery_info['driver_id']
                if delivery_status['scheduled_count'] == 0 and delivery_status['missed_count'] == 0:
                    # Delivery has no scheduled or missed orders, so it's completed
                    delivery_info['status'] = 'COMPLETED'
                    delivery_info['driver_id'] = ''
                    # Mark driver as available
                    driver = fetch_tb_endpoint(tb_info, 'drivers_api', {'driver_id': driver_id})[0]
                    driver['status'] = 'AVAILABLE'
                    batch_update_drivers.append(driver)
                    batch_update_deliveries.append(delivery_info)
                elif delivery_status['missed_count'] > 0:
                    # Delivery has missed orders, so mark as delayed even if it has scheduled orders
                    delivery_info['status'] = 'DELAYED'
                    batch_update_deliveries.append(delivery_info)
                else:
                    # Delivery has scheduled orders but no missed orders, so it's still in progress
                    pass
        batch_update(tb_info, "cdc_deliveries", batch_update_deliveries, 'delivery_id', 'deliveries_api')
        batch_update(tb_info, "cdc_drivers", batch_update_drivers, 'driver_id', 'drivers_api')

def process_orders_for_delivery(tb_info, delivery):
    # Fetch current status of orders in delivery
    orders = fetch_tb_endpoint(tb_info, 'orders_api', {'orders_in': ','.join(delivery['orders'])})
    out = []
    driver_id = delivery['driver_id']
    # Parse orders into active, missed, and pending
    active, missed, pending = parse_delivery_slots(orders)
    logging.info(f"Delivery {delivery['delivery_id']} has {len(orders)} orders in total, {len(active)} in current slot, {len(missed)} missed slots, and {len(pending)} pending slots.")
    if active:
        logging.info(f"Processing {len(active)} orders for delivery...")
        # Time to deliver some orders, issue a driver ENROUTE event
        send_driver_status_event(tb_info, driver_id, 'ENROUTE')
        # Sort orders by delivery slot start time
        active.sort(key=lambda o: o['delivery_slot_start'])
        # For each order, generate a delivery event
        for order in active:
            # Decide if this order was missed
            if fake.random.random() < DELIVERY_MISS_RATE:
                logging.info(f"Order {order['order_id']} was missed.")
                missed.append(order)
                continue
            else:
                # First send real-time event to driver_events table
                send_driver_status_event(tb_info, driver_id, 'DELIVERED', order['order_id'])
                # Then prepare batch update to cdc_orders table for later
                order['status'] = 'DELIVERED'
                order['delivery_completed_at'] = format_datetime(current_time())
                out.append(order)
                sleep(MINIMUM_TS_BOUNDARY)  # Ensure timestamps aren't in the same moment
        # Finally, send driver STOPPED event
        send_driver_status_event(tb_info, driver_id, 'STOPPED')
    else:
        logging.info(f"No orders to deliver for Delivery {delivery['delivery_id']}.")
    if missed:
        logging.info(f"Driver {driver_id} has {len(missed)} orders to reschedule.")
        # For each order, generate a delivery event
        for order in missed:
            # First send real-time event to driver_events table
            send_driver_status_event(tb_info, driver_id, 'MISSED', order['order_id'])
            # Then prepare batch update to cdc_orders table for later
            order['status'] = 'MISSED'
            out.append(order)
            sleep(MINIMUM_TS_BOUNDARY)  # Ensure timestamps aren't in the same moment
    if pending:
        logging.info(f"Skipping {len(pending)} orders for Delivery {delivery['delivery_id']}.")
    return out

def process_active_drivers(tb_info, active_drivers):
    # Prepare batch update to cdc_orders table
    batch_order_updates = []
    for driver in active_drivers:
        # Fetch delivery info - driver would be unassigned on completion
        delivery_info = fetch_tb_endpoint(tb_info, 'deliveries_api', {'driver_id': driver['driver_id']})
        # Check that driver only has one delivery
        if delivery_info and len(delivery_info) > 1:
            raise Exception(f"Driver {driver['driver_id']} has more than one delivery assigned.")
        elif not delivery_info:
            logging.info(f"Driver {driver['driver_id']} has no active delivery.")
        else:
            logging.info(f"Driver {driver['driver_id']} has active delivery {delivery_info[0]['delivery_id']}")
            delivery = delivery_info[0]
            # Check if we're in the delivery slot
            delivery_route_start = parse_datetime(delivery['start_time'])
            if delivery_route_start > current_time():
                logging.info(f"Delivery {delivery['delivery_id']} is before delivery window start time, skipping...")
            else:
                logging.info(f"Delivery {delivery['delivery_id']} is within delivery window.")
                orders_update = process_orders_for_delivery(tb_info, delivery)
                batch_order_updates += orders_update
                
    # Check if we have any orders to batch update
    batch_update(tb_info, "cdc_orders", batch_order_updates, 'order_id', 'orders_api')

def process_operations_tasks(tb_info):
    ops_tasks = fetch_tb_endpoint(tb_info, 'operations_api')
    if not ops_tasks:
        logging.info("No operations tasks found to process.")
    else:
        for task in ops_tasks:
            logging.info(f"Processing operations task {task['event_id']}...")
            orders_details = fetch_tb_endpoint(tb_info, 'orders_api', {'orders_in': ','.join(task['event_info'])})
            # Create an array of orders per delivery_id
            deliveries_to_prune = {}
            for order in orders_details:
                if order['delivery_id'] not in deliveries_to_prune:
                    deliveries_to_prune[order['delivery_id']] = []
                deliveries_to_prune[order['delivery_id']].append(order)
            # Fetch each delivery and prune the orders list to remove the orders we've just processed
            deliveries_batch_update = []
            for delivery_id, orders_to_prune in deliveries_to_prune.items():
                delivery = fetch_tb_endpoint(tb_info, 'deliveries_api', {'delivery_id': delivery_id})[0]
                delivery['orders'] = [o for o in delivery['orders'] if o not in orders_to_prune]
                delivery['status'] = 'SCHEDULED'
                deliveries_batch_update.append(delivery)
            # Generate new delivery for these orders
            generate_delivery(tb_info, orders=orders_details)
            # Update cdc_deliveries table to remove the processed orders
            batch_update(tb_info, "cdc_deliveries", deliveries_batch_update, 'delivery_id', 'deliveries_api')
            # Mark task as completed
            task['event_type'] = 'COMPLETED'
            batch_update(tb_info, "operations_events", [task], 'event_id', 'operations_api', ts_field='event_time')

def runner():
    logging.info("Reading Tinybird Connection info...")
    tb_info = read_tb_info()
    # Start outer loop
    while True:
        # Start loop timer
        start_time = current_time()
        # Ensure active deliveries
        ensure_active_deliveries(tb_info)
        # Handle rescheduling requests
        process_operations_tasks(tb_info)
        # Fetch active drivers from Tinybird
        logging.info("Fetching active drivers from Tinybird...")
        active_drivers = fetch_tb_endpoint(tb_info, 'drivers_api', {'status_in': 'ASSIGNED,ENROUTE,STOPPED'})
        if not active_drivers:
            logging.info("No active drivers found.")
        else:
            logging.info(f"{len(active_drivers)} active drivers found.")
            process_active_drivers(tb_info, active_drivers)
        # Determine if any deliveries are now compeleted given the processed order updates
        process_delivery_status_updates(tb_info)
        # Calculate time taken
        end_time = current_time()
        time_taken = end_time - start_time
        logging.info(f"Loop completed in {time_taken.total_seconds()} seconds.")

if __name__ == '__main__':
    runner()
