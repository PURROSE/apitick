import os
import dearpygui.dearpygui as dpg

def setup():
    dpg.create_context()
    dpg.create_viewport(title='apiTick', width=600, height=600)

    ico_path = os.path.abspath(r".\static\favicon.ico")
    dpg.set_viewport_large_icon(ico_path)
    dpg.set_viewport_small_icon(ico_path)

    dpg.set_exit_callback(on_exit)

    # 加载中文字体
    with dpg.font_registry():
        with dpg.font(r".\static\SIMSUN.TTC", 18) as font:
            dpg.add_font_range_hint(dpg.mvFontRangeHint_Default)
            dpg.add_font_range_hint(dpg.mvFontRangeHint_Chinese_Simplified_Common)
            dpg.add_font_range_hint(dpg.mvFontRangeHint_Chinese_Full)
    dpg.bind_font(font)

    width, height = dpg.get_viewport_client_width(), dpg.get_viewport_client_height()

    with dpg.window(
        label="Main Window", 
        tag="main_window",
        no_title_bar=True,      # 无标题栏
        no_move=True,           # 不可移动
        no_resize=True,         # 不可缩放
        no_close=True,           # 无关闭按钮
        no_collapse=True,       # 不可折叠
        autosize=True,
        width=width,
        height=height,
        pos=(0, 0),
        # no_scrollbar=False,
        ):
        dpg.add_button(label="添加新按钮", callback=add_new_button_callback)

    dpg.set_viewport_resize_callback(on_viewport_resize)

    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.start_dearpygui()

def on_exit():
    dpg.destroy_context()
def add_new_button_callback():
    dpg.add_button(label="动态按钮", parent="main_window")
    

def on_viewport_resize(sender, app_data):
    dpg.configure_item("main_window", width=app_data[0], height=app_data[1])
