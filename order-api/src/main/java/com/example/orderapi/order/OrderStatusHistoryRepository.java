package com.example.orderapi.order;


import org.springframework.data.jpa.repository.JpaRepository;


public interface OrderStatusHistoryRepository extends JpaRepository<OrderStatusHistory, Long> {}