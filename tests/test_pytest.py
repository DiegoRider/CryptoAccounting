from sources.classes import BaseToken, Buy


def test_basetoken():

    cvx = BaseToken("cvx", Buy('cvx', 100, 5))
    cvx.add_buy(20, 12)
    
    assert cvx.amount() == 120
    
    removed = cvx.remove(10)

    assert cvx.amount() == 110
    assert removed.amount() == 10

    cvx.remove(20)

    assert cvx.amount() == 90

