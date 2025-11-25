#!/usr/bin/env node
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import archiver from "archiver";
import { MigrationConfig } from "./config.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ──────────────────────── НАСТРОЙКИ ────────────────────────
const START_DATE = "2025-09-01";
const END_DATE = "2025-09-01";
const BATCH_SIZE = 10000;
const OUT_DIR = path.resolve(__dirname, "OUT");

// === УПРАВЛЯЮЩИЕ КОНСТАНТЫ ===
const PACK_INTO_ZIP = true;
const FILENAME_COUNTERPARTIES = "counterparties.xml";
const FILENAME_INVOICES = "invoices.xml";
const USE_PERIOD_IN_FILENAME = true;

// ─────────────────────────────────────────────────────────────

if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

const green = t => `\x1b[32m${t}\x1b[0m`;
const yellow = t => `\x1b[33m${t}\x1b[0m`;
const cyan = t => `\x1b[36m${t}\x1b[0m`;
const red = t => `\x1b[31m${t}\x1b[0m`;
const gray = t => `\x1b[90m${t}\x1b[0m`;

// SQL
const sqlInvoicesPath = path.resolve(__dirname, "SQL/invoice-period.json.sql");
const sqlContragsPath = path.resolve(__dirname, "SQL/invoice-counterpatries-period.sql");
if (!fs.existsSync(sqlInvoicesPath) || !fs.existsSync(sqlContragsPath)) {
  console.error(red("Не найден один из SQL-файлов!"));
  process.exit(1);
}

const sqlInvoices = fs.readFileSync(sqlInvoicesPath, "utf-8");
const sqlContrags = fs.readFileSync(sqlContragsPath, "utf-8");

const today = new Date().toISOString().slice(0, 10);
const periodSuffix = USE_PERIOD_IN_FILENAME ? `_${START_DATE}_to_${END_DATE}` : "";
const zipFilename = `commerce${periodSuffix}.zip`;
const zipPath = path.join(OUT_DIR, zipFilename);

const escapeXml = (s) => s == null ? "" : String(s)
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;")
  .replace(/'/g, "&apos;");

async function main() {
  const overallStart = process.hrtime.bigint(); // ← точное время старта

  console.log(cyan(`\nВыгрузка CommerceML 2.10 за период ${yellow(START_DATE)} → ${yellow(END_DATE)}\n`));
  console.log(cyan(`Режим: ${PACK_INTO_ZIP ? "в ZIP" : "отдельные файлы"} | Файлы: ${yellow(FILENAME_COUNTERPARTIES)} + ${yellow(FILENAME_INVOICES)}\n`));

  const client = new Client(MigrationConfig.pg);
  await client.connect();
  console.log(green("Подключено к PostgreSQL\n"));

  let archive = null;
  let outputStream = null;

  if (PACK_INTO_ZIP) {
    if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);
    outputStream = fs.createWriteStream(zipPath);
    archive = archiver("zip", { zlib: { level: 9 } });

    // Важно: слушаем ошибки
    archive.on("error", err => { throw err; });
    archive.pipe(outputStream);
  }

  try {
    await client.query("BEGIN");
    await client.query("DECLARE contrag_cursor NO SCROLL CURSOR FOR " + sqlContrags, [START_DATE, END_DATE]);
    await client.query("DECLARE inv_cursor NO SCROLL CURSOR FOR " + sqlInvoices, [START_DATE, END_DATE]);

    // ───── Контрагенты ─────
    const cpContent = [
      `<?xml version="1.0" encoding="utf-8"?>`,
      `<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="${today}">`,
      `  <Контрагенты>`
    ];

    console.log(cyan(`Генерация ${FILENAME_COUNTERPARTIES}...`));
    let totalContrags = 0;

    while (true) {
      const res = await client.query(`FETCH FORWARD ${BATCH_SIZE} FROM contrag_cursor`);
      if (res.rowCount === 0) break;

      for (const c of res.rows) {
        const isSeller = c.id.startsWith("seller_");
        cpContent.push(`    <Контрагент>
      <Ид>${escapeXml(c.id)}</Ид>
      <Наименование>${escapeXml(c.full_name || "—")}</Наименование>
      <ПолноеНаименование>${escapeXml(c.full_name || "—")}</ПолноеНаименование>
      ${c.inn ? `<ИНН>${escapeXml(c.inn)}</ИНН>` : ""}
      ${c.kpp ? `<КПП>${escapeXml(c.kpp)}</КПП>` : ""}
      ${c.ogrn ? `<ОГРН>${escapeXml(c.ogrn)}</ОГРН>` : ""}
      <ЮридическийАдрес><Представление>${escapeXml(c.address || "—")}</Представление></ЮридическийАдрес>
      ${c.rs ? `<РасчетныеСчета>
        <РасчетныйСчет>
          <НомерСчета>${escapeXml(c.rs)}</НомерСчета>
          <Банк>
            <Наименование>${escapeXml(c.bank_name || "—")}</Наименование>
            <БИК>${escapeXml(c.bik || "")}</БИК>
            <КоррСчет>${escapeXml(c.ks || "")}</КоррСчет>
          </Банк>
        </РасчетныйСчет>
      </РасчетныеСчета>` : ""}
      <Роль>${isSeller ? "Продавец" : "Покупатель"}</Роль>
    </Контрагент>`);
        totalContrags++;
      }
    }

    cpContent.push(`  </Контрагенты>`, `</КоммерческаяИнформация>`);
    const counterpartiesXml = cpContent.join("\n");

    if (PACK_INTO_ZIP) {
      archive.append(counterpartiesXml, { name: FILENAME_COUNTERPARTIES });
    } else {
      fs.writeFileSync(path.join(OUT_DIR, FILENAME_COUNTERPARTIES), counterpartiesXml, "utf-8");
    }

    await client.query("CLOSE contrag_cursor");
    console.log(green(`${FILENAME_COUNTERPARTIES} → ${yellow(totalContrags)} контрагентов\n`));

    // ───── Счета ─────
    const invContent = [
      `<?xml version="1.0" encoding="utf-8"?>`,
      `<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="${today}">`,
      `  <Документы>`
    ];

    console.log(cyan(`Генерация ${FILENAME_INVOICES}...`));
    let totalInvoices = 0;
    let batch = 0;

    while (true) {
      batch++;
      const batchStart = Date.now();
      const res = await client.query(`FETCH FORWARD ${BATCH_SIZE} FROM inv_cursor`);
      if (res.rowCount === 0) break;

      for (const row of res.rows) {
        const inv = row.invoice_json;
        invContent.push(`    <Документ>
      <Ид>${escapeXml(inv.inv_id)}</Ид>
      <Номер>${escapeXml(inv.inv_nomer)}</Номер>
      <Дата>${inv.inv_date}</Дата>
      <ХозОперация>Счет на оплату</ХозОперация>
      <Роль>Продавец</Роль>
      <Валюта>RUB</Валюта>
      <Курс>1</Курс>
      <Сумма>${inv.inv_cost}</Сумма>
      <Контрагенты>
        <Контрагент><Ид>${escapeXml(inv.seller_id)}</Ид><Роль>Продавец</Роль></Контрагент>
        <Контрагент><Ид>${escapeXml(inv.buyer_id)}</Ид><Роль>Покупатель</Роль></Контрагент>
      </Контрагенты>
      <Комментарий>${escapeXml(inv.inv_mem || "")}</Комментарий>
      <Товары>${inv.DatasArray.map(item => `
        <Товар>
          <Ид>${inv.inv_id}-${item.Pos}</Ид>
          <Наименование>${escapeXml(item.Name)}</Наименование>
          <Количество>${item.Amount}</Количество>
          <ЦенаЗаЕдиницу>${item.Price}</ЦенаЗаЕдиницу>
          <Сумма>${item.Cost}</Сумма>
          <Единица>${escapeXml(item.mU)}</Единица>
          <СтавкаНДС>без НДС</СтавкаНДС>
        </Товар>`).join("")}
      </Товары>
    </Документ>`);
        totalInvoices++;
      }

      const elapsed = Date.now() - batchStart;
      console.log(`Пакет ${yellow(batch.toString().padStart(3))} | Счетов: ${yellow(res.rowCount.toString().padStart(5))} | ${yellow(elapsed + "мс")} | Всего: ${gray(totalInvoices.toLocaleString())}`);
    }

    invContent.push(`  </Документы>`, `</КоммерческаяИнформация>`);
    const invoicesXml = invContent.join("\n");

    if (PACK_INTO_ZIP) {
      archive.append(invoicesXml, { name: FILENAME_INVOICES });
    } else {
      fs.writeFileSync(path.join(OUT_DIR, FILENAME_INVOICES), invoicesXml, "utf-8");
    }

    await client.query("CLOSE inv_cursor");
    await client.query("COMMIT");

    // ───── Финализация архива с информированием пользователя ─────
    if (PACK_INTO_ZIP) {
      console.log(cyan("\nЗавершаем архив и записываем на диск... (это может занять несколько секунд)"));
      await archive.finalize();
      await new Promise((resolve, reject) => {
        outputStream.on("close", resolve);
        outputStream.on("error", reject);
      });
    }

    // ───── Общее время выполнения ─────
    const overallEnd = process.hrtime.bigint();
    const totalMs = Number(overallEnd - overallStart) / 1_000_000;
    const totalSec = (totalMs / 1000).toFixed(2);

    console.log("\n" + "═".repeat(90));
    console.log(green("ГОТОВО!"));
    console.log(green(`Контрагентов: ${yellow(totalContrags.toLocaleString())} | Счетов: ${yellow(totalInvoices.toLocaleString())}`));
    console.log(green(`Общее время выполнения: ${yellow(totalSec + " сек")}`));
    if (PACK_INTO_ZIP) {
      console.log(green(`ZIP создан: ${yellow(zipFilename)}`));
      console.log(green(`  ├─ ${FILENAME_COUNTERPARTIES}`));
      console.log(green(`  └─ ${FILENAME_INVOICES}`));
    } else {
      console.log(green(`Файлы сохранены в папке OUT:`));
      console.log(green(`  ├─ ${FILENAME_COUNTERPARTIES}`));
      console.log(green(`  └─ ${FILENAME_INVOICES}`));
    }
    console.log("═".repeat(90) + "\n");

  } catch (err) {
    await client.query("ROLLBACK");
    console.error(red("\nОШИБКА — транзакция откатана"));
    console.error(red(err.stack || err));
    process.exit(1);
  } finally {
    await client.end();
  }
}

main().catch(err => {
  console.error(red("\nКРИТИЧЕСКАЯ ОШИБКА:"));
  console.error(err);
  process.exit(1);
});