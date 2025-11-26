#!/usr/bin/env node
import { Client } from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import archiver from "archiver";
import { MigrationConfig } from "./config.js";
import { XMLParser } from "fast-xml-parser";
import libxml from "libxmljs2";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ──────────────────────── НАСТРОЙКИ ────────────────────────
const START_DATE = "2000-09-01";
const END_DATE = "2026-01-01";
const BATCH_SIZE = 10000;
const OUT_DIR = path.resolve(__dirname, "OUT");

// === УПРАВЛЯЮЩИЕ КОНСТАНТЫ ===
const PACK_INTO_ZIP = false;
const FILENAME_COUNTERPARTIES = "counterparties.xml";
const FILENAME_INVOICES = "invoices.xml";
const USE_PERIOD_IN_FILENAME = true;
const VALIDATE_XML = true; // ← Включить/выключить валидацию

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

function getCurrentDateTime() {
  const now = new Date();

  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  const seconds = String(now.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
}

const today = getCurrentDateTime();
const periodSuffix = USE_PERIOD_IN_FILENAME ? `_${START_DATE}_to_${END_DATE}` : "";
const zipFilename = `commerce${periodSuffix}.zip`;
const zipPath = path.join(OUT_DIR, zipFilename);

const escapeXml = (s) => s == null ? "" : String(s)
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;")
  .replace(/'/g, "&apos;");

// Функция валидации XML по XSD
function validateXmlWithXsd(xmlContent, xsdContent, documentType) {
  try {
    const xmlDoc = libxml.parseXml(xmlContent);
    const xsdDoc = libxml.parseXml(xsdContent);

    const validationResult = xmlDoc.validate(xsdDoc);

    if (!validationResult) {
      const errors = xmlDoc.validationErrors.map(err => `  - ${err}`).join('\n');
      console.error(red(`❌ Ошибки валидации ${documentType}:\n${errors}`));
      return false;
    }

    console.log(green(`✅ ${documentType} валидирован успешно`));
    return true;
  } catch (error) {
    console.error(red(`❌ Ошибка при валидации ${documentType}: ${error.message}`));
    return false;
  }
}

// Функция быстрой проверки XML (синтаксис)
function validateXmlSyntax(xmlContent, documentType) {
  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      parseAttributeValue: true,
      parseTagValue: true
    });

    const result = parser.parse(xmlContent);
    console.log(green(`✅ ${documentType} синтаксис корректен`));
    return true;
  } catch (error) {
    console.error(red(`❌ Ошибка синтаксиса в ${documentType}: ${error.message}`));
    return false;
  }
}

async function main() {
  const overallStart = process.hrtime.bigint();

  console.log(cyan(`\nВыгрузка CommerceML 2.10 за период ${yellow(START_DATE)} → ${yellow(END_DATE)}\n`));
  console.log(cyan(`Режим: ${PACK_INTO_ZIP ? "в ZIP" : "отдельные файлы"} | Валидация: ${VALIDATE_XML ? yellow("ВКЛ") : gray("ВЫКЛ")}\n`));

  // Загрузка XSD схемы
  let xsdContent = null;
  if (VALIDATE_XML) {
    try {
      const xsdPath = path.resolve(__dirname, "commerceml.xsd");
      if (fs.existsSync(xsdPath)) {
        xsdContent = fs.readFileSync(xsdPath, "utf-8");
        console.log(green("✅ XSD схема загружена"));
      } else {
        console.log(yellow("⚠️  XSD файл не найден, будет выполнена только синтаксическая проверка"));
      }
    } catch (error) {
      console.log(yellow(`⚠️  Не удалось загрузить XSD: ${error.message}`));
    }
  }

  const client = new Client(MigrationConfig.pg);
  await client.connect();
  console.log(green("Подключено к PostgreSQL\n"));

  let archive = null;
  let outputStream = null;

  if (PACK_INTO_ZIP) {
    if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);
    outputStream = fs.createWriteStream(zipPath);
    archive = archiver("zip", { zlib: { level: 9 } });

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
      `<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="${today}" xmlns="urn:1C.ru:commerceml_2">`,
      `  <Документ>`,
      `    <Ид>counterparties</Ид>`,
      `    <Номер>1</Номер>`,
      `    <Дата>${today.slice(0, 10)}</Дата>`,
      `    <ХозОперация>Прочие</ХозОперация>`,
      `    <Роль>Продавец</Роль>`,
      `    <Валюта>RUB</Валюта>`,
      `    <Курс>1</Курс>`,
      `    <Сумма>0</Сумма>`,
      ``,
      `    <Контрагенты>`
    ];

    console.log(cyan(`Генерация ${FILENAME_COUNTERPARTIES}...`));
    let totalContrags = 0;

    while (true) {
      const res = await client.query(`FETCH FORWARD ${BATCH_SIZE} FROM contrag_cursor`);
      if (res.rowCount === 0) break;

      for (const c of res.rows) {
        const isSeller = c.id.startsWith("seller_");

        const inn = c.inn.length === 10 || c.inn.length === 12 ?
          `<ИНН>${escapeXml(c.inn)}</ИНН>` :
          "";

        const kpp = c.kpp.length === 9 ?
          `<КПП>${escapeXml(c.kpp)}</КПП>` :
          "";


        const ks = c.ks.length === 20 ?
          `<СчетКорреспондентский>${escapeXml(c.ks)}</СчетКорреспондентский>` :
          "";

        const bank = c.bik.length === 9 ?
          `<Банк>
            ${ks}
            <Наименование>${escapeXml(c.bank_name || "—")}</Наименование>
            <БИК>${escapeXml(c.bik)}</БИК>
          </Банк>` :
          "";
        const rs = bank && c.rs && c.rs.length === 20 ?
          `<РасчетныйСчет>
          <НомерСчета>${escapeXml(c.rs)}</НомерСчета>
          ${bank}
        </РасчетныйСчет>` :
          "";

        const memo = c.memo ?
          `<ЗначениеРеквизита><Наименование>Комментарий</Наименование><Значение>${escapeXml(c.memo)}</Значение></ЗначениеРеквизита>` :
          "";

        const name = c.inn.length === 10 ?
          `<ОфициальноеНаименование>${escapeXml(c.full_name || "—")}</ОфициальноеНаименование>` :
          `<ПолноеНаименование>${escapeXml(c.full_name || "—")}</ПолноеНаименование>`;

        const address = c.address ?
          `<Адрес><Представление>${escapeXml(c.address)}</Представление></Адрес>` :
          "";

        const orgn = c.ogrn && c.ogrn.length === 13 ?
          `<ЗначенияРеквизитов><ЗначениеРеквизита><Наименование>${c.ogrn.startsWith('3') ? "ОГРНИП" : "ОГРН"}</Наименование><Значение>${escapeXml(c.ogrn)}</Значение></ЗначениеРеквизита></ЗначенияРеквизитов>` :
          "";

        const rekv = orgn || memo ?
          `<ЗначенияРеквизитов>
        ${orgn}
        ${memo}
      </ЗначенияРеквизитов>` :
          "";

        cpContent.push(`
      <Контрагент>
        <Ид>${escapeXml(c.id)}</Ид>
        ${name}
        ${inn}
        ${kpp}
        ${address}
        <Роль>${isSeller ? "Продавец" : "Покупатель"}</Роль>
        ${rs}
      </Контрагент>`);
        totalContrags++;
      }
    }

    cpContent.push(
      `  </Контрагенты>`,
      `  </Документ>`,
      `</КоммерческаяИнформация>`
    );
    const counterpartiesXml = cpContent.join("\n");

    // Валидация контрагентов
    if (VALIDATE_XML) {
      console.log(cyan("\nВалидация контрагентов..."));
      const syntaxValid = validateXmlSyntax(counterpartiesXml, "Контрагенты");

      if (xsdContent && syntaxValid) {
        const schemaValid = validateXmlWithXsd(counterpartiesXml, xsdContent, "Контрагенты");
        if (!schemaValid) {
          throw new Error("Валидация контрагентов по XSD не пройдена");
        }
      } else if (!syntaxValid) {
        throw new Error("Синтаксическая проверка контрагентов не пройдена");
      }
    }

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
      `<КоммерческаяИнформация ВерсияСхемы="2.10" ДатаФормирования="${today}" xmlns="urn:1C.ru:commerceml_2">`
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
        <Контрагент><Ид>${escapeXml(inv.seller_id)}</Ид><ПолноеНаименование/><Роль>Продавец</Роль></Контрагент>
        <Контрагент><Ид>${escapeXml(inv.buyer_id)}</Ид><ПолноеНаименование/><Роль>Покупатель</Роль></Контрагент>
      </Контрагенты>
      <Комментарий>${escapeXml(inv.inv_mem || "")}</Комментарий>
      <Товары>${inv.DatasArray.map(item => `
        <Товар>
          <Ид>${inv.inv_id}-${item.Pos}</Ид>
          <Наименование>${escapeXml(item.Name.slice(0, 255))}</Наименование>
          <БазоваяЕдиница Код="${item.mUcode}">${escapeXml(item.mU)}</БазоваяЕдиница>
          ${item.Name.length > 255 ? `<Описание>${escapeXml(item.Name)}</Описание>` : ""}
          <ЦенаЗаЕдиницу>${item.Price}</ЦенаЗаЕдиницу>
          <Количество>${item.Amount}</Количество>
          <Сумма>${item.Cost}</Сумма>
        </Товар>`).join("")}
      </Товары>
    </Документ>`);
        totalInvoices++;
      }

      const elapsed = Date.now() - batchStart;
      console.log(`Пакет ${yellow(batch.toString().padStart(3))} | Счетов: ${yellow(res.rowCount.toString().padStart(5))} | ${yellow(elapsed + "мс")} | Всего: ${gray(totalInvoices.toLocaleString())}`);
    }

    invContent.push(`</КоммерческаяИнформация>`);
    const invoicesXml = invContent.join("\n");

    // Валидация счетов
    if (VALIDATE_XML) {
      console.log(cyan("\nВалидация счетов..."));
      const syntaxValid = validateXmlSyntax(invoicesXml, "Счета");

      if (xsdContent && syntaxValid) {
        const schemaValid = validateXmlWithXsd(invoicesXml, xsdContent, "Счета");
        if (!schemaValid) {
          throw new Error("Валидация счетов по XSD не пройдена");
        }
      } else if (!syntaxValid) {
        throw new Error("Синтаксическая проверка счетов не пройдена");
      }
    }

    if (PACK_INTO_ZIP) {
      archive.append(invoicesXml, { name: FILENAME_INVOICES });
    } else {
      fs.writeFileSync(path.join(OUT_DIR, FILENAME_INVOICES), invoicesXml, "utf-8");
    }

    await client.query("CLOSE inv_cursor");
    await client.query("COMMIT");

    // ───── Финализация архива ─────
    if (PACK_INTO_ZIP) {
      console.log(cyan("\nЗавершаем архив и записываем на диск..."));
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
    if (VALIDATE_XML) {
      console.log(green(`Валидация: ${yellow("пройдена успешно")}`));
    }
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