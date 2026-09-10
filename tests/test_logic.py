from src.consumer import FailureSimulator, RunningAverage, ProcessingFailure, PermanentProcessingFailure


def test_running_average():
    avg = RunningAverage()
    assert avg.add(100.0) == 100.0
    assert avg.add(200.0) == 150.0
    assert round(avg.add(50.0), 2) == 116.67


def test_retry_item_succeeds_on_third_attempt():
    sim = FailureSimulator()
    order = {"orderId": "r1", "product": "RetryItem", "price": 10.0}
    try:
        sim.process(order)
    except ProcessingFailure:
        pass
    try:
        sim.process(order)
    except ProcessingFailure:
        pass
    sim.process(order)
    assert sim.attempts["r1"] == 3


def test_dlq_item_is_permanent():
    sim = FailureSimulator()
    order = {"orderId": "d1", "product": "DLQItem", "price": 10.0}
    try:
        sim.process(order)
        assert False, "Expected permanent failure"
    except PermanentProcessingFailure:
        assert sim.attempts["d1"] == 1
