function dump_obj(myObject) {
    var s = "";
    for (var property in myObject) {
        s = s + "\n "+property +": " + myObject[property] ;
    }
    alert(s);
}

function postNewAssignment() {
    var name = $("#assignment_name").val();
    var configuration = $("#assignment_body").val();
    if (name.length === 0) {
        alert("assignment name can't be empty!");
        return;
    }
    if (configuration.length === 0) {
        alert("assignment body can't be empty!");
        return;
    }
    $.ajax({
        type: 'post',
        url: '/api/zhuque/' + name,
        data: configuration,
        dataType: "json",
        success: function (data) {
            var state = data.state;
            if (state === 400) {
                alert(data.body);
                return;
            }
            alert("create assignment success!");
        },
        error: function () {
            alert("unknown error!");
        }
    });
}

$(document).ready(function () {
    $("#new_assignment").click(function () {
        postNewAssignment();
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