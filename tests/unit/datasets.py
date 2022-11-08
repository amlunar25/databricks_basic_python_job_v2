from basic_python_job.datasets import car_journey, customer, order

def test_datasets():
    assert len(car_journey.get_initial_records())>0
    assert len(customer.get_initial_records())>0
    assert len(order.get_initial_records())>0