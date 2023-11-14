import os
import requests
import json
import faker
import logging
import datetime
from time import sleep

TARGET_ACTIVE_DELIVERIES = 5
MAX_ORDERS_PER_DELIVERY = 10
MAX_ORDERS_TO_PROCESS_PER_RUN = 5
TARGET_AVAILABLE_CUSTOMERS = 50
TARGET_AVAILABLE_DRIVERS = 10
DELIVERY_TIME_INTERVAL = datetime.timedelta(seconds=15)

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
        return datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        return datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

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
        post_data_to_tb(tb_info, 'customers_raw', new_customers)
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
        post_data_to_tb(tb_info, 'drivers_raw', new_drivers)
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

def generate_delivery_and_orders(tb_info, customers, drivers):
    logging.info(f"Generating new a delivery and orders and assigning a driver...")
    delivery_id = fake.uuid4()
    driver = drivers[0]
    num_orders = fake.random_int(min=1, max=MAX_ORDERS_PER_DELIVERY)
    start_time = current_time()
    delivery = {
        "delivery_id": delivery_id,
        "driver_id": driver['driver_id'],
        "start_time": format_datetime(start_time),
        "status": "SCHEDULED",
        "updated_at": format_datetime(current_time())
    }
    orders = []
    for _ in range(num_orders):
        order_id = fake.uuid4()
        delivery_slot = start_time + _ * DELIVERY_TIME_INTERVAL
        orders.append({
            "order_id": order_id,
            "customer_id": customers[_]['customer_id'],
            "delivery_id": delivery_id,
            "status": "SCHEDULED",
            "delivery_slot_start": format_datetime(delivery_slot),
            "delivery_slot_end": format_datetime(delivery_slot + DELIVERY_TIME_INTERVAL),
            "delivery_completed_at": "",
            "updated_at": format_datetime(current_time())
        })
    
    delivery["orders"] = [order["order_id"] for order in orders]

    driver["status"] = "ASSIGNED"
    driver["updated_at"] = format_datetime(current_time())

    # Submit updates to Tinybird
    logging.info("Posting new delivery to Tinybird...")
    post_data_to_tb(tb_info, "deliveries_raw", [delivery])
    logging.info("Posting new orders to Tinybird...")
    post_data_to_tb(tb_info, "orders_raw", orders)
    logging.info("Posting driver assignment to Tinybird...")
    post_data_to_tb(tb_info, 'drivers_raw', [driver])
    sleep(1)  # Ensure timestamps aren't in the same second
    # Wait for update propagation
    wait_for_update(tb_info, delivery, 'delivery_id', 'deliveries_api')
    wait_for_update(tb_info, orders[-1], 'order_id', 'orders_api')
    wait_for_update(tb_info, driver, 'driver_id', 'drivers_api')
    logging.info("New delivery and orders propagated to Tinybird.")

def wait_for_update(tb_info, obj, status_field, endpoint):
    obj_id = obj[status_field]
    expected_updated_at = parse_datetime(obj['updated_at'])
    logging.info(f"Waiting for {status_field} {obj_id} to have 'updated_at' >= {expected_updated_at}")

    while True:
        latest_record = fetch_tb_endpoint(tb_info, endpoint, {status_field: obj_id})
        if not latest_record:
            logging.info(f"{status_field} {obj_id} not found in endpoint {endpoint}. Waiting...")
            sleep(1)
            continue

        latest_updated_at = parse_datetime(latest_record[0]['updated_at'])
        if latest_updated_at >= expected_updated_at:
            logging.info(f"{status_field} {obj_id} is up to date with 'updated_at' = {latest_updated_at}")
            break
        else:
            logging.info(f"Latest 'updated_at' {latest_updated_at} is before expected {expected_updated_at}. Waiting...")
            sleep(1)

def ensure_active_deliveries(tb_info):
    logging.info("Fetching Current Deliveries from Tinybird...")
    active_deliveries = fetch_tb_endpoint(tb_info, 'deliveries_api', {'status': 'SCHEDULED'})
    current_delivery_count = len(active_deliveries)
    logging.info(f"{current_delivery_count} active deliveries found.")
    # If current deliveries is less than target, generate new deliveries
    if current_delivery_count < TARGET_ACTIVE_DELIVERIES:
        # Ensure we have a pool of customers ready to receive orders
        logging.info("Fetching Customers without active orders from Tinybird...")
        customers = ensure_available_customers(tb_info)
        drivers = ensure_available_drivers(tb_info)
        # Generate a new delivery and orders
        generate_delivery_and_orders(tb_info, customers, drivers)
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

def batch_update(tb_info, table_name, items, id_field, api_endpoint):
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
        item['updated_at'] = format_datetime(current_time())

    # Post batch update to Tinybird
    logging.info(f"Posting batch updates to {table_name} in Tinybird...")
    post_data_to_tb(tb_info, table_name, items)
    wait_for_update(tb_info, items[-1], id_field, api_endpoint)

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
        batch_update(tb_info, "deliveries_raw", batch_update_deliveries, 'delivery_id', 'deliveries_api')
        batch_update(tb_info, "drivers_raw", batch_update_drivers, 'driver_id', 'drivers_api')

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
            # First send real-time event to driver_events table
            send_driver_status_event(tb_info, driver_id, 'DELIVERED', order['order_id'])
            # Then prepare batch update to orders_raw table for later
            order['status'] = 'DELIVERED'
            order['delivery_completed_at'] = format_datetime(current_time())
            out.append(order)
            sleep(.1)  # Ensure timestamps aren't in the same moment
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
            # Then prepare batch update to orders_raw table for later
            order['status'] = 'MISSED'
            out.append(order)
            sleep(.1)  # Ensure timestamps aren't in the same moment
    if pending:
        logging.info(f"Skipping {len(pending)} orders for Delivery {delivery['delivery_id']}.")
    return out

def process_active_drivers(tb_info, active_drivers):
    # Prepare batch update to orders_raw table
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
    batch_update(tb_info, "orders_raw", batch_order_updates, 'order_id', 'orders_api')

def runner():
    logging.info("Reading Tinybird Connection info...")
    tb_info = read_tb_info()
    # Start outer loop
    while True:
        # Ensure active deliveries
        ensure_active_deliveries(tb_info)
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

if __name__ == '__main__':
    runner()
