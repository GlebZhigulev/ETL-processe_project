db = db.getSiblingDB('app_source');
db.createCollection('user_sessions');
db.createCollection('event_logs');
db.createCollection('support_tickets');