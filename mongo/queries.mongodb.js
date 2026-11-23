// Consultas de validación para mongosh / MongoDB Compass / extensión de VS Code.
//   docker exec -it mongodb mongosh -u admin -p <password> --authenticationDatabase admin qcommerce
// y pega las consultas.

use("qcommerce");

// Conteo por colección
["orders", "metrics_by_window", "store_kpis", "orders_rejected"].forEach(c =>
  print(c, db.getCollection(c).countDocuments()));

// Un documento de orden (modelo documental con sub-documentos)
db.orders.find().sort({ event_ts: -1 }).limit(1);

// Aggregation pipeline: ingresos y tasa de cancelación por región
db.orders.aggregate([
  { $group: {
      _id: "$country_region",
      orders: { $sum: 1 },
      revenue: { $sum: "$amounts.net_amount" },
      cancelled: { $sum: { $cond: [{ $eq: ["$order_status", "CANCELLED"] }, 1, 0] } } } },
  { $project: {
      orders: 1,
      revenue: { $round: ["$revenue", 2] },
      cancel_rate_pct: { $round: [{ $multiply: [{ $divide: ["$cancelled", "$orders"] }, 100] }, 2] } } },
  { $sort: { revenue: -1 } }
]);

// Aggregation pipeline: categoría más vendida por tier de tienda
db.orders.aggregate([
  { $group: { _id: { tier: "$store.tier", cat: "$product_category" },
              revenue: { $sum: "$amounts.net_amount" } } },
  { $sort: { revenue: -1 } },
  { $group: { _id: "$_id.tier", top_category: { $first: "$_id.cat" },
              revenue: { $first: { $round: ["$revenue", 2] } } } }
]);

// Últimas ventanas de 1 minuto
db.metrics_by_window.find({}, { _id: 0, window_start: 1, country_region: 1,
  product_category: 1, total_orders: 1, total_revenue: 1 }).sort({ window_start: -1 }).limit(10);

// Mensajes corruptos aislados por el consumidor
db.orders_rejected.aggregate([{ $group: { _id: "$reject_reason", n: { $sum: 1 } } }]);
