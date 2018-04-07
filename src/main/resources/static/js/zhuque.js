function getAssignmentName() {
    return $("#assignment_name").val();
}

function getAssignmentConfiguration() {
    return $("#assignment_body").val();
}

$(document).ready(function () {
    $("#new_assignment").click(function () {
        var name = getAssignmentName();
        var configuration = getAssignmentConfiguration();
    });
    $("#get_all_assignment").click(function () {
        var name = getAssignmentName();
        //get all assignment
    });
    $("#get_assignment").click(function () {
        var name = getAssignmentName();
        //get assignment
    });
    $("#delete_assignment").click(function () {
        var name = getAssignmentName();
        //delete assignment
    })
})