from __future__ import annotations

from typing import Any, Dict, List

INDUSTRY_PACKS: Dict[str, List[Dict[str, Any]]] = {
    "home_services": [
        {"query": "Property Management", "enabled": True},
        {"query": "HOA Management", "enabled": True},
        {"query": "Commercial Real Estate", "enabled": True},
        {"query": "Insurance Agencies", "enabled": True},
        {"query": "Home Insurance", "enabled": True},
        {"query": "Self Storage", "enabled": True},
        {"query": "Moving Companies", "enabled": True},
        {"query": "Roofing", "enabled": True},
        {"query": "Plumbers", "enabled": True},
        {"query": "Electricians", "enabled": True},
        {"query": "Contractors", "enabled": True},
        {"query": "Pest Control", "enabled": True},
    ],
    "home_services_light": [
        {"query": "Property Management", "enabled": True},
        {"query": "Home Insurance", "enabled": True},
        {"query": "Self Storage", "enabled": True},
        {"query": "Moving Companies", "enabled": True},
        {"query": "Roofing", "enabled": True},
        {"query": "Plumbers", "enabled": True},
        {"query": "Electricians", "enabled": True},
        {"query": "Contractors", "enabled": True},
        {"query": "Pest Control", "enabled": True},
        {"query": "Tree Service", "enabled": True},
    ],
    "tourism_services": [
        {"query": "Property Management", "enabled": True},
        {"query": "Self Storage", "enabled": True},
        {"query": "Moving Companies", "enabled": True},
        {"query": "Carpet Cleaning", "enabled": True},
        {"query": "Pest Control", "enabled": True},
        {"query": "AC Repair", "enabled": True},
        {"query": "Garage Door Repair", "enabled": True},
        {"query": "Tree Service", "enabled": True},
        {"query": "Contractors", "enabled": True},
    ],
    "real_estate_agents": [
        {"query": "Real Estate Agents", "enabled": True},
        {"query": "Real Estate Buyer Brokers", "enabled": True},
        {"query": "Real Estate Consultants", "enabled": True},
        {"query": "Commercial Real Estate", "enabled": True},
        {"query": "Property Management", "enabled": True},
        {"query": "Home Insurance", "enabled": True},
        {"query": "Self Storage", "enabled": True},
        {"query": "Moving Companies", "enabled": True},
        {"query": "Real Estate Referral & Information Service", "enabled": True},
    ],
    "car_dealerships": [
        {"query": "New Car Dealers", "enabled": True},
        {"query": "Used Car Dealers", "enabled": True},
        {"query": "Automobile & Truck Brokers", "enabled": True},
        {"query": "Wholesale Used Car Dealers", "enabled": True},
    ],
}

__all__ = ["INDUSTRY_PACKS"]