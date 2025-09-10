# deliverywatch

# 1 заказ в Берлине
scripts/simulate_order.sh

# 3 заказа в разных городах в параллели
CITY=Berlin scripts/simulate_order.sh &
CITY=Paris  scripts/simulate_order.sh &
CITY=Rome   scripts/simulate_order.sh &
wait