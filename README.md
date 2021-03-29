# Distributed Systems (Spring 2020)

This repository contains projects completed as a part of the coursework for Distributed Systems (Spring 2020) taught by Prof. Steve Ko at the University at Buffalo.

## Prerequisites
To run the projects, installing Android Studio is a pre-requisite as the individual nodes in the different distributed system scenarios are represented by Android Virtual Devices (AVD) and the communication between these systems is done using Java Socket Programming.

## Projects
1. Simple Messenger - A simple messenger application enabling two Android devices to send messages to each other. 


2. Group Messenger - A messenger applicaiton enabling 5 Android devices to send messages among each other, with Total and FIFO ordering guarentees. This project simulates a typical messenger application like Whatsapp, Telegram etc.


3. Simple DHT - A simple Distributed Hash Table based on the Chord protocol. This project implements the following
   - ID space partitioning / re-partitioning
   - Ring based routing
   - Handling new nodes joining and nodes leaving the system

4. Simple Amazon Dynamo DB - A simple implementation of the Relicated Key-Value store explained in Amazon's Dynamo DB paper. This projects implements the following main components. 

   - Partitioning
   - Replication
   - Failure handling (Atmost 1 node can fail at any given time)

## Usage
- Create AVDs using the `Scripts/create_avd.py` file.
- Run 5 AVDs using the `Scripts/run_avd.py` file.
- Enable communication between AVDs using the `Scripts/set_redir.py` file.
- To check if the implementation of a project is correct run the corresponding Grading scripts for the project.
