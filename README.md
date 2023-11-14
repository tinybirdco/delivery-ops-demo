# Real-time Delivery Operations Demo

## Overview

This project demonstrates a real-time data processing application using Tinybird and Retool, focusing on a simulated environment for delivery operations. The application simulates delivery drivers for a any logistics service, managing customers, orders, and delivery schedules.

Most importantly it simulates a typical two-speed service where the transactional system is minutes or hours behind the real events, and a stream of updates directly from the driver is used to allow the customer service centre to provide real-time operational services.

## Features

- **Customer and Driver Simulation**: Generates customers and drivers, ensuring a pool of available entities for order processing.
- **Order and Delivery Management**: Automatically creates and assigns orders to deliveries, managing delivery slots and statuses.
- **Real-time Event Handling**: Simulates real-world events like delivery completions and driver delays, updating the system in real-time.
- **Notification System**: Notifies relevant stakeholders (e.g., customers) in case of any changes or delays in the delivery schedule.
- **Real-time Analytics**: Provides real-time analytics on the delivery operations, including order and delivery counts, delivery statuses, and more.

## Requirements
You will need a Tinybird Workspace, a Retool Project, and Python 3.7+ installed on your machine.

## Setup

1. Clone this repository to your local machine.
2. Create a Tinybird Workspace and a Retool Project.
3. Run `tb auth` in this repository to authenticate your Tinybird account.
4. Run `tb push` to push the data project to your Tinybird Workspace.
5. Install the Python requirements with `pip install -r requirements.txt`.
6. Run `python datagen.py` to start the simulation.
7. Open the Retool Project and start using the application.