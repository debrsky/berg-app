// validate.js
import { parseXmlString } from 'libxmljs2';
import { readFileSync } from 'fs';
import { resolve } from 'path';

// Путь к XSD-схеме (относительно текущего файла)
const SCHEMA_PATH = resolve(import.meta.dirname || process.cwd(), 'commerceml.xsd');

// Список XML-файлов для проверки
// Укажите нужные файлы здесь или импортируйте из другого места
const FILES = [
  //'OUT/invoice.example.xml',
  //'OUT/counterparties.xml',
  'OUT/invoices.xml',
];

/**
 * Валидация одного XML-файла по CommerceML XSD
 * @param {string} filePath
 */
function validateFile(filePath) {
  const absolutePath = resolve(import.meta.dirname || process.cwd(), filePath);

  try {
    const xmlContent = readFileSync(absolutePath, 'utf-8');
    const xsdContent = readFileSync(SCHEMA_PATH, 'utf-8');

    const xmlDoc = parseXmlString(xmlContent);
    const xsdDoc = parseXmlString(xsdContent);

    const isValid = xmlDoc.validate(xsdDoc);

    if (isValid) {
      console.log(`[Valid] ${filePath}`);
    } else {
      console.error(`[Invalid] ${filePath}`);
      xmlDoc.validationErrors.forEach((err, idx) => {
        console.error(`  Ошибка ${idx + 1}: ${err.message.trim()} (строка ${err.line}, столбец ${err.column})`);
      });
    }
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.error(`[Error] Файл не найден: ${filePath}`);
    } else {
      console.error(`[Error] Ошибка при обработке ${filePath}:`, err.message);
    }
  }
}

// Основной запуск
if (FILES.length === 0) {
  console.warn('Warning: Список FILES пустой — ничего не будет проверено.');
} else {
  console.log(`Запуск валидации ${FILES.length} файл(ов) по схеме ${SCHEMA_PATH}\n`);
  FILES.forEach(validateFile);
  console.log('\nВалидация завершена.');
}