/** @odoo-module **/

import { registry } from "@web/core/registry";
import { listView } from "@hr_expense/views/list";


const expenseView = registry.category("views").get("hr_expense_dashboard_tree");
console.log(expenseView)
expenseView.buttonTemplate = 'qweb_js.ListButtons'