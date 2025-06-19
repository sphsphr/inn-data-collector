import pandas as pd
import time
import json
import os
import glob
import logging
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from sqlalchemy import create_engine, Column, String, Integer, Table, ForeignKey
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError


# FILE_PATH = r"\\MAX\Users\crysi\inn_table\inn_table3.xlsx"
print(
    "Укажите путь к excel таблице из сетевой или обычной папки(например "
    r"\\MAX\Users\crysi\inn_table\inn_table2.xlsx):",
    end="",
)
FILE_PATH = input()


class Config:
    # Путь для сохранения PDF
    PDF_OUTPUT_DIR = os.path.join(os.getcwd(), "REPORTS")

    # Настройки базы данных
    DB_URL = "sqlite:///test_db.sqlite"

    # Настройки Selenium
    SELENIUM_WINDOW_SIZE = (1240, 1080)
    SELENIUM_TIMEOUT = 10

    # Настройки логирования
    LOG_FILE = "last_run_log.log"
    LOG_LEVEL = logging.INFO


def setup_logging():
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(Config.LOG_FILE),
            logging.StreamHandler(),
        ],
    )


# ---Создаём базу данных---
Base = declarative_base()


class LegalEntity(Base):
    __tablename__ = "legal_entities"
    inn = Column(String(12), primary_key=True)
    ip_name = Column(String)
    ogrn = Column(String)
    bankruptcy_cases = Column(String)
    ip_pdf_path = Column(String)
    name_full = Column(String)
    name_short = Column(String)
    fio = Column(String)
    okato = Column(String)
    oktmo = Column(String)
    okpo = Column(String)
    address = Column(String)
    status = Column(String)

    cases = relationship(
        "BankruptcyCase", back_populates="legal_entity", cascade="all, delete-orphan"
    )


class BankruptcyCase(Base):
    __tablename__ = "bankruptcy_cases"
    case_number = Column(String(20), primary_key=True)  # Номер дела
    inn = Column(String(12), ForeignKey("legal_entities.inn"))  # Связь с ИП
    claimant_name = Column(String)
    judge_name = Column(String)
    creditors = Column(String)
    third_parties = Column(String)
    others = Column(String)
    # Связь с основной таблицей
    legal_entity = relationship("LegalEntity", back_populates="cases")


engine = create_engine(Config.DB_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)


# ---Функция для сохранения в БД всех полученных данных об ИНН---
def save_to_db(data):
    try:
        session = Session()
        if session.query(LegalEntity).filter_by(inn=data["inn"]).first():
            logging.warning(f"Дубликат ИНН {data['inn']}")
        existing_entity = (
            session.query(LegalEntity).filter_by(inn=str(data["inn"])).first()
        )
        if existing_entity:
            # Удаляем связанные дела о банкротстве
            session.query(BankruptcyCase).filter_by(inn=str(data["inn"])).delete()
            session.delete(existing_entity)
            session.commit()
            logging.info(
                f"Удалены старые данные для ИНН {data["inn"]} перед обновлением"
            )

        entity = LegalEntity(
            inn=data.get("inn"),
            ip_name=data.get("ip_name"),
            ogrn=data.get("ogrn"),
            ip_pdf_path=data.get("ip_pdf_path"),
            name_full=data.get("name_full"),
            name_short=data.get("name_short"),
            fio=data.get("fio"),
            okato=data.get("okato"),
            oktmo=data.get("oktmo"),
            okpo=data.get("okpo"),
            address=json.dumps(data.get("address"), ensure_ascii=False),
            status=data.get("status"),
        )
        case_numbers = []
        for i in range(len(data["bankruptcy_cases"])):
            case = BankruptcyCase(
                case_number=data["bankruptcy_cases"][i]["case_number"],
                inn=data["inn"],
                claimant_name=data["bankruptcy_cases"][i]["claimant_name"],
                judge_name=data["bankruptcy_cases"][i]["judge_name"],
                creditors="; ".join(data["bankruptcy_cases"][i]["creditors"]),
                third_parties="; ".join(data["bankruptcy_cases"][i]["third_parties"]),
                others="; ".join(data["bankruptcy_cases"][i]["others"]),
            )
            case_numbers.append(case.case_number)
            session.add(case)
        entity.bankruptcy_cases = ", ".join(case_numbers)
        session.add(entity)
        session.commit()
        return True

    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Ошибка сохранения ИНН {data.get('inn')} в БД: {str(e)}")
        return False
    finally:
        session.close()


# ---Функция для сохранения веб-страницы в файл .pdf---
def save_as_pdf(url, output_dir, filename="document.pdf") -> str:
    try:
        os.makedirs(output_dir, exist_ok=True)
        # Конфигурация печати
        settings = {
            "recentDestinations": [
                {"id": "Save as PDF", "origin": "local", "account": ""}
            ],
            "selectedDestinationId": "Save as PDF",
            "version": 2,
        }

        prefs = {
            "printing.print_preview_sticky_settings.appState": json.dumps(settings),
            "savefile.default_directory": output_dir,
            "savefile.default_filename": filename,
        }

        # Настройки Chrome
        chrome_options = Options()
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--kiosk-printing")

        driver = webdriver.Chrome(options=chrome_options)
        driver.set_window_size(*Config.SELENIUM_WINDOW_SIZE)
        driver.get(url)
        # Ждем полной загрузки страницы, sleep нужен для pdf
        wait_for_element(driver, "//div[contains(text(),'ОГРН')]")
        time.sleep(3)
        # Жмем кнопку принять cookie, чтобы она не блокировала обзор
        cookie_button_accept = driver.find_element(
            By.XPATH,
            "//button[@class='btn-accept btn-accept_hover btn-accept_properties']",
        )
        cookie_button_accept.click()
        # Сохраняем страницу
        driver.execute_script("window.print();")
        time.sleep(1)
        pdf_files = glob.glob(os.path.join(output_dir, "*"))
        # Получаем самый свежий файл по ключу
        latest_pdf = max(pdf_files, key=os.path.getmtime)

        # Задаем новое имя
        final_path = os.path.join(output_dir, filename)

        # Удаляем старый файл с новым именем, если существует
        if final_path in pdf_files:
            os.remove(final_path)
        # Переименовываем
        os.rename(latest_pdf, final_path)
    except Exception as e:
        logging.error(f"Ошибка при сохранении PDF: {str(e)}")
        return None
    finally:
        driver.quit()
        return final_path


# Проверяем номер ИНН на валидность
def validate_inn(inn) -> bool:
    inn_str = str(inn)
    if not inn_str.isdigit() or len(inn_str) not in (10, 12):
        raise ValueError(f"Неверный формат ИНН: {inn}")
    return True


# Читаем excel с ИНН
def read_excel_from_network_folder(file_path: str) -> list:
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        inn_list = df["ИНН"].tolist()  # предполагаем, что столбец называется ИНН
        return inn_list
    except Exception as e:
        logging.error(f"Ошибка чтения файла Excel: {str(e)}")
        return []


# Для удобства использования WebDriverWait
def wait_for_element(
    driver,
    xpath: str,
    timeout: int = Config.SELENIUM_TIMEOUT,
    condition: str = "presence",
    raise_exception: bool = True,
):
    conditions = {
        "presence": EC.presence_of_element_located,
        "visible": EC.visibility_of_element_located,
        "clickable": EC.element_to_be_clickable,
        "text_in_element": EC.text_to_be_present_in_element,
    }

    if condition not in conditions:
        raise ValueError(f"Неподдерживаемый тип ожидания: {condition}")

    try:
        return WebDriverWait(driver, timeout).until(
            conditions[condition]((By.XPATH, xpath))
        )
    except TimeoutException:
        if raise_exception:
            raise
        return None


# Сбора информации с fedresurs
def check_fedresurs(inn) -> dict:
    result = {
        "ip_name": None,
        "inn": None,
        "ogrn": None,
        "bankruptcy_cases": [],
        "ip_pdf_path": None,
    }
    try:
        driver = webdriver.Chrome()
        driver.set_window_size(*Config.SELENIUM_WINDOW_SIZE)
        # Заходим на сайт, ждем загрузки
        driver.get("https://bankrot.fedresurs.ru/bankrupts?searchString")
        wait_for_element(driver, "//div[@class='u-card-result__wrapper']")

        # Поиск по ИНН
        search_input = driver.find_element(
            By.XPATH,
            "//input[@formcontrolname='searchString']",
        )
        search_input.send_keys(inn)

        search_button = driver.find_element(By.XPATH, "//button[@class='el-button']")
        search_button.click()

        # Проверка наличия результатов
        results = wait_for_element(driver, "//el-tab-panel")
        if not results:
            logging.info(f"Для ИНН {inn} нет данных на Fedresurs")
            return result
        # Сбор информации об ИП
        try:
            result["ip_name"] = driver.find_element(
                By.XPATH,
                "//div[@class='u-card-result__name u-card-result__name_mb u-card-result__name_width']/span",
            ).text
            result["ogrn"] = driver.find_element(
                By.XPATH, "//span[contains(text(), 'ОГРН')]/following-sibling::span"
            ).text
            result["inn"] = driver.find_element(
                By.XPATH, "//span[contains(text(), 'ИНН')]/following-sibling::span"
            ).text
        except NoSuchElementException as e:
            logging.warning(f"Не удалось извлечь данные для ИНН {inn}: {str(e)}")

        # Переход на страницу с подробной информацией
        wait_for_element(driver, "//el-info-link")
        more_info_button = driver.find_element(By.XPATH, "//el-info-link")
        more_info_button.click()
        time.sleep(0.5)

        # Переключаемся на новую вкладку
        all_tabs = driver.window_handles
        driver.switch_to.window(all_tabs[-1])
        WebDriverWait(driver, 10).until(
            EC.text_to_be_present_in_element(
                (By.XPATH, "//entity-card-biddings-block/div/div/div"),
                "Продажа имущества",
            )
        )

        # Сохраняем страницу об ИП в .pdf
        result["ip_pdf_path"] = save_as_pdf(
            url=driver.current_url,
            output_dir=Config.PDF_OUTPUT_DIR,
            filename=f"{inn}.pdf",
        )
        # Проверяем наличие дел о банкротстве
        bankruptcy_cases_availability = wait_for_element(
            driver, "//a[@class='underlined info-header']"
        )
        if not bankruptcy_cases_availability:
            logging.info(f"Для ИНН {inn} нет дел о банкротстве на Fedresurs")
            return result
        # Собираем данные о банкротстве
        try:
            bankruptcy_cases_raw = driver.find_elements(
                By.XPATH, "//a[@class='underlined info-header']"
            )
            result["bankruptcy_cases"] = [case.text for case in bankruptcy_cases_raw]
        except TimeoutException:
            logging.warning(f"Не удалось найти дела о банкротстве для ИНН {inn}")
    except Exception as e:
        logging.error(f"Ошибка при проверке ИНН {inn} на Fedresurs: {str(e)}")
    finally:
        driver.quit()
        return result


def check_kad_arbitr(bankruptcy_case: str) -> dict:
    result = {
        "case_number": bankruptcy_case,
        "claimant_name": None,
        "judge_name": None,
        "creditors": [],
        "third_parties": [],
        "others": [],
    }
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = webdriver.Chrome(options=options)
        driver.set_window_size(*Config.SELENIUM_WINDOW_SIZE)
        driver.get("https://kad.arbitr.ru/")

        # Закрытие всплывающего окна
        popup_window_close = wait_for_element(
            driver,
            "//a[@class='b-promo_notification-popup-close js-promo_notification-popup-close']",
            raise_exception=False,
        )
        if popup_window_close:
            popup_window_close.click()

        # Поиск дела
        search_input = wait_for_element(
            driver, "//input[@placeholder='например, А50-5568/08']"
        )
        search_input.send_keys(bankruptcy_case)
        search_button = driver.find_element(By.XPATH, "//button[@alt='Найти']")
        search_button.click()

        # Проверка наличия результатов
        results = wait_for_element(
            driver, "//div[@class='judge']", raise_exception=False
        )
        if not results:
            logging.info(f"Дело {bankruptcy_case} не найдено на Kad.Arbitr")
            return result
        # Собираем информацию о судье и истце
        try:
            result["judge_name"] = driver.find_element(
                By.XPATH,
                "//div[@class='judge']",
            ).text
            result["claimant_name"] = driver.find_element(
                By.XPATH, "//td[@class='plaintiff']/div/div/span"
            ).text
        except Exception as e:
            logging.error(f"Ошибка при поиске судьи или истца: {str(e)}")

        # Кликаем на дело
        case_link_button = driver.find_element(By.XPATH, "//a[@class='num_case']")
        if case_link_button:
            case_link_button.click()
            time.sleep(0.2)
            all_tabs = driver.window_handles
            driver.switch_to.window(all_tabs[-1])
            wait_for_element(driver, "//td[@class='plaintiffs first']//a")

            # Переходим на страницу с печатью, где находится полный список всех лиц
            print_people_button = driver.find_element(
                By.XPATH,
                "//li[@class='case-print']//a",
            )
            if print_people_button:
                print_people_button.click()
            wait_for_element(driver, "//li[@class='chrono active']")

            creditors_raw = driver.find_elements(
                By.XPATH, "//td[@class='plaintiffs first']//li/span"
            )
            if creditors_raw:
                result["creditors"] = [creditor.text for creditor in creditors_raw]

            third_parties_raw = driver.find_elements(
                By.XPATH, "//td[@class='third']//li/span"
            )
            if third_parties_raw:
                result["third_parties"] = [
                    third_face.text for third_face in third_parties_raw
                ]

            others_raw = driver.find_elements(
                By.XPATH, "//td[@class='others']//li/span"
            )
            if others_raw:
                result["others"] = [other.text for other in others_raw]
    except Exception as e:
        logging.error(f"Ошибка при проверке дела {bankruptcy_case}: {str(e)}")
    finally:
        driver.quit()
        return result


def check_inn_with_dadata(inn) -> dict:
    result = {
        "name_full": None,
        "name_short": None,
        "fio": None,
        "okato": None,
        "oktmo": None,
        "okpo": None,
        "address": None,
        "status": None,
    }
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = webdriver.Chrome(options=options)
        driver.set_window_size(*Config.SELENIUM_WINDOW_SIZE)
        driver.get("https://dadata.ru/api/find-party/")

        # Редактирование и отправка запроса
        edit_request = driver.find_element(
            By.XPATH,
            """//div[contains(text(), '"query":')]""",
        )
        edit_request.click()
        edit_request.send_keys(Keys.CONTROL + "a")
        edit_request.send_keys('{ "query": "', inn, '" }')
        send_button = driver.find_element(
            By.XPATH,
            "//button[@data-test='sandbox-btn']",
        )
        send_button.click()

        # Ожидание запроса
        time.sleep(1.5)
        wait_for_element(driver, "//pre[@data-test='sandbox-results']")

        # Парсинг JSON ответа
        json_data = json.loads(
            driver.find_element(By.XPATH, "//pre[@data-test='sandbox-results']").text
        )
        suggestion = json_data.get("suggestions", [{}])[0]
        data = suggestion.get("data", {})
        name_info = data.get("name", {})
        state_info = data.get("state", {})
        address_info = data.get("address", {})
        founder_info = (
            data.get("founders", [{}])[0].get("fio", {}) if data.get("founders") else {}
        )
        result.update(
            {
                "name_full": name_info.get("full_with_opf"),
                "name_short": name_info.get("short_with_opf"),
                "fio": (
                    " ".join(
                        filter(
                            None,
                            [
                                founder_info.get("surname"),
                                founder_info.get("name"),
                                founder_info.get("patronymic"),
                            ],
                        )
                    )
                    if founder_info
                    else None
                ),
                "okato": data.get("okato"),
                "oktmo": data.get("oktmo"),
                "okpo": data.get("okpo"),
                "address": address_info.get("data"),
                "status": state_info.get("status"),
            }
        )
    except Exception as e:
        logging.error(f"Ошибка при проверке ИНН {inn} через DaData: {str(e)}")
    finally:
        driver.quit()
        return result


def main():
    setup_logging()
    logging.info("Запуск обработки ИНН")

    # Чтение ИНН из файла
    inn_list = read_excel_from_network_folder(FILE_PATH)
    if not inn_list:
        logging.error("Не удалось прочитать ИНН из файла или файл пуст")
        return

    logging.info(f"Найдено {len(inn_list)} ИНН для обработки")
    for inn in inn_list:
        try:
            logging.info(f"Обработка ИНН: {inn}")
            validate_inn(inn)

            # Сбор данных с Fedresurs
            fedresurs_data = check_fedresurs(inn)
            if not fedresurs_data.get("ip_name") or not fedresurs_data.get(
                "bankruptcy_cases"
            ):
                logging.warning(f"Не найдено данных для ИНН {inn} на Fedresurs")
                continue

            # Сбор данных о делах о банкротстве
            kad_arbitr_data = []
            for case in fedresurs_data["bankruptcy_cases"]:
                kad_arbitr_data.append(check_kad_arbitr(case))

            # Сбор данных через DaData
            dadata_data = check_inn_with_dadata(inn)

            # Объединение данных
            combined_data = {
                "ip_name": fedresurs_data.get("ip_name"),
                "inn": fedresurs_data.get("inn"),
                "ogrn": fedresurs_data.get("ogrn"),
                "bankruptcy_cases": kad_arbitr_data,
                "name_full": dadata_data.get("name_full"),
                "name_short": dadata_data.get("name_short"),
                "fio": dadata_data.get("fio"),
                "okato": dadata_data.get("okato"),
                "oktmo": dadata_data.get("oktmo"),
                "okpo": dadata_data.get("okpo"),
                "address": dadata_data.get("address"),
                "status": dadata_data.get("status"),
                "ip_pdf_path": fedresurs_data.get("ip_pdf_path"),
            }
            # Сохранение в БД
            if save_to_db(combined_data):
                logging.info(f"Данные для ИНН {inn} успешно сохранены")
            else:
                logging.error(f"Не удалось сохранить данные для ИНН {inn}")

        except ValueError as e:
            logging.error(f"Неверный ИНН {inn}: {str(e)}. ИНН будет пропущен.")
            continue
        except Exception as e:
            logging.error(f"Ошибка при обработке ИНН {inn}: {str(e)}.")
            continue
        finally:
            logging.info(f"Конец проверки {inn}")


if __name__ == "__main__":
    main()
