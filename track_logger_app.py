import streamlit as st

from src.spotify_logger.config import get_config
from src.spotify_logger.registry import ensure_registry_entry, set_registry_enabled
from src.spotify_logger.sheets_client import extract_sheet_id, open_user_sheet
from src.spotify_logger.sheet_structure import (
    read_app_state,
    validate_log_headers,
    fix_log_headers,
    prepare_user_sheet,
)
from src.spotify_logger.worker import sync_single_user


st.set_page_config(layout="centered", page_title="Spotify Track Logger")


def show_service_account_info():
    cfg = get_config()
    with st.expander("Google Service Account"):
        st.write("Share your Google Sheet with this email as **Editor**:")
        st.code(cfg.google_service_account_email, language="text")


def format_status_from_state(state) -> str:
    parts = [
        f"enabled={state.enabled}",
        f"timezone={state.timezone}",
        f"last_synced_after_ts={state.last_synced_after_ts}",
        f"updated_at={state.updated_at}",
    ]
    if state.last_error:
        parts.append(f"last_error={state.last_error}")
    return " | ".join(parts)


def main():
    st.markdown("## Spotify Track Logger")
    st.write(
        "Вставь ссылку или ID своей Google Sheet, расшарь её на сервисный аккаунт, "
        "подключи Spotify и мы будем автоматически логировать твои прослушивания."
    )

    show_service_account_info()

    sheet_input = st.text_input("Google Sheet URL или ID", key="sheet_url")
    timezone = st.text_input("Timezone (IANA, например Europe/Amsterdam)", value="UTC", key="timezone")

    if not sheet_input:
        st.stop()

    sheet_id = extract_sheet_id(sheet_input)

    col1, col2 = st.columns([1, 1])
    with col1:
        check_btn = st.button("Check access / Prepare sheet")
    with col2:
        fix_headers_btn = st.button("Fix log headers")

    ss = None
    state = None

    if check_btn:
        try:
            ss = open_user_sheet(sheet_id)
            ensure_registry_entry(sheet_id)
            prepare_user_sheet(ss, timezone=timezone or "UTC")
            is_valid, _ = validate_log_headers(ss)
            if is_valid:
                st.success("Таблица подготовлена ✅")
            else:
                st.warning("log содержит другие заголовки. Нажми 'Fix headers', чтобы привести в нужный формат.")
            state = read_app_state(ss)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Не удалось открыть/подготовить таблицу: {exc}")

    if fix_headers_btn:
        try:
            ss = open_user_sheet(sheet_id)
            fix_log_headers(ss)
            st.success("Заголовки log приведены к нужному формату ✅")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Ошибка при фиксе заголовков: {exc}")

    # Если уже есть app_state — показать текущий статус и действия
    if ss is None:
        try:
            ss = open_user_sheet(sheet_id)
        except Exception:
            ss = None

    if ss is not None:
        if state is None:
            try:
                state = read_app_state(ss)
            except Exception:
                state = None

    if state is not None:
        st.markdown("### Status")
        if state.enabled and state.refresh_token_enc and state.spotify_user_id:
            st.success("У тебя уже всё подключено ✅")
        st.text(format_status_from_state(state))

        col_run, col_enable, col_disable = st.columns(3)
        with col_run:
            if st.button("Run sync now"):
                try:
                    sync_single_user(sheet_id)
                    state = read_app_state(ss)
                    st.success("Синк выполнен.")
                    st.text(format_status_from_state(state))
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Ошибка во время синка: {exc}")
        with col_enable:
            if st.button("Enable logging"):
                try:
                    set_registry_enabled(sheet_id, True)
                    from src.spotify_logger.sheet_structure import write_app_state

                    write_app_state(ss, {"enabled": "true"})
                    st.success("Логирование включено.")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Не удалось включить логирование: {exc}")
        with col_disable:
            if st.button("Disable logging"):
                try:
                    set_registry_enabled(sheet_id, False)
                    from src.spotify_logger.sheet_structure import write_app_state

                    write_app_state(ss, {"enabled": "false"})
                    st.success("Логирование выключено.")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Не удалось выключить логирование: {exc}")

        st.markdown("### Spotify connection")
        st.info(
            "Подключение/переподключение Spotify (OAuth) пока нужно сделать вручную, "
            "используя Authorization Code Flow и записав refresh_token в __app_state.refresh_token_enc "
            "через шифрование Fernet. Следующий шаг — обернуть это в удобный UI."
        )


if __name__ == "__main__":
    main()

