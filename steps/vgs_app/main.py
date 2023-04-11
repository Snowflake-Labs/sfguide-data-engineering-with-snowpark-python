import streamlit as st

dummy_table = [
    {
        'name': 'orange',
        'label': 'Confidential',
        'reveals_to': 'elevated_role_name_x',
    },
    {
        'name': 'red',
        'label': 'Restricted',
        'reveals_to': 'elevated_role_name_1',
    },
    {
        'name': 'red',
        'label': 'Restricted',
        'reveals_to': 'elevated_role_name_2',
    }
]


def validate_connection():
    # return 0/0
    return

def create_row_ui(row, index):
    ' Takes each element from `dummy_table` and generates the corresponding expander with the desired information '
    row['buttons'] = 'buttons'
    with st.expander(row['name'].capitalize(), expanded=True):
        col1, col2, col3 = st.columns([2, 2, 1])

        def handle_label(key, value):
            with col1:
                st.markdown(f"**{key.capitalize()}**")
                st.markdown(value)

        def handle_reveals_to(key, value):
            with col2:
                st.markdown(f"**{key.capitalize().replace('_', ' ')}**")
                st.markdown(value)

        def handle_buttons(key, value):
            with col3:
                st.button('Delete', key=f'btn_{index}_delete', type='primary', use_container_width=True)
                st.button('Edit', key=f'btn_{index}_edit', type='primary', use_container_width=True)

        handlers = {
            'label': handle_label,
            'reveals_to': handle_reveals_to,
            'buttons': handle_buttons,
        }

        # Calls each function
        for key, value in row.items():
            handlers.get(key, lambda k, v: None)(key, value)

def layout_components(components):
    ' Loops list of dicts and creates components based on their type '
    for component in components:
        if component['type'] == 'text_input':
            st.text_input(label = component['label'])
        if component['type'] == 'selectbox':
            st.selectbox(label = component['label'], options = component['options'])

# Page configuration
st.set_page_config(
    layout="centered"
)

# Placeholder for connect page container, to delete once connected
connect_container_placeholder = st.empty()

with connect_container_placeholder.container():
    st.subheader('Connect to VGS')

    # Connect page input fields
    connect_fields = [
        {'type': 'text_input', 'label': 'VGS Organization ID'},
        {'type': 'text_input', 'label': 'VGS Vault ID'},
        {'type': 'text_input', 'label': 'Client ID'},
        {'type': 'text_input', 'label': 'Client Secret'}
    ]

    # Building user input components
    layout_components(connect_fields)

    #Align button to right side
    col1, col2 = st.columns([3, 1])
    with col2:
            connect = st.button('Connect', type = 'primary', use_container_width = True)

if connect:
    #We have to validate connection
    try:
        validate_connection()
    except:
        st.error('Connection failed')
    else:
        #Delete connect container
        # connect_container_placeholder.empty()
        
        st.subheader('Data Classification Levels')
        data_class_lvl = [
            {'type': 'text_input', 'label': 'Name'},
            {'type': 'text_input', 'label': 'Label'},
            {'type': 'selectbox', 'label': 'Reveal to', 'options' : ['elevated_role_name', 'Dummy', 'Other Dummy']}
        ]

        # Building user input components
        layout_components(data_class_lvl)

        #Align button to right side
        col1, col2 = st.columns([3, 1])
        with col2:
            st.button('Add/Update', type = 'primary', use_container_width = True)

        # Loops tru dummy table and calls the function to create the ui 
        # sends the index on enumerate to prevent duplicate widgets
        for index, row in enumerate(dummy_table):
            create_row_ui(row, index)
